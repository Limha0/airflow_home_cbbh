import logging
import urllib3

from pendulum import datetime, now
from airflow.decorators import dag, task, task_group
from util.common_util import CommonUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dag(
    dag_id="sdag_api_to_dw_hour",
    schedule="20 */1 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["api_to_dw", "hour", "ext"],
)
def api_to_dw_hour():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    @task
    def insert_collect_data_info(**kwargs):
        """
        tn_data_bsc_info 테이블에서 수집 대상 기본 정보 조회 후 th_data_clct_mastr_log 테이블에 입력
        return: collect_data_list
        """
        # 수집 대상 기본 정보 조회
        select_bsc_info_stmt = '''
                            SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                            FROM tn_data_bsc_info
                            WHERE 1=1
                                AND LOWER(link_yn) = 'y'
                                AND LOWER(link_clct_mthd_dtl_cd) = 'open_api'
                                AND LOWER(link_clct_cycle_cd) = 'hour'
                                AND link_ntwk_otsd_insd_se = '외부'
                                AND LOWER(dtst_cd) = 'data852' -- 기상청_단기예보
                            ORDER BY sn
                            '''
        data_interval_start = now()  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = now()  # 실제 실행하는 날짜를 KST 로 설정
        collect_data_list = CommonUtil.insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs)
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return collect_data_list
    
    def format_value(value):
        return f"'{value}'" if isinstance(value, str) else str(value)

    @task_group(group_id='call_url_process')
    def call_url_process(collect_data_list):
        from util.file_util import FileUtil

        @task
        def create_directory(collect_data_list, **kwargs):
            """
            수집 파일 경로 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info
            return: file_path: tn_clct_file_info 테이블에 저장할 파일 경로
            """
            data_interval_end = now()  # 실제 실행하는 날짜를 KST 로 설정
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, data_interval_end, root_collect_file_path, "n")
            return file_path
        
        @task
        def call_url(collect_data_list, file_path, **kwargs):
            """
            조건별 URL 설정 및 호출하여 dw 적재
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
            return: file_size
            """
            import requests
            import os
            import time
            from xml_to_dict import XMLtoDict
            from util.call_url_util import CallUrlUtil
            from util.date_custom_util import DateUtil

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']

            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            link_se_cd = tn_data_bsc_info.link_se_cd.lower()
            pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
            pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()
            base_url = return_url = tn_data_bsc_info.link_data_clct_url

            # 파라미터 및 파라미터 길이 설정
            data_interval_start = now()  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
            data_interval_end = now()  # 실제 실행하는 날짜를 KST 로 설정
            params_dict, params_len = CallUrlUtil.set_params(tn_data_bsc_info, session, data_interval_start, data_interval_end, kwargs)

            retry_num = 0  # 데이터 없을 시 재시도 횟수
            repeat_num = 1  # 파라미터 길이만큼 반복 호출 횟수
            page_no = 1  # 현재 페이지
            total_page = 1  # 총 페이지 수
            header = False
            mode = "w"  # 파일 쓰기 모드 overwrite
            
            try:
                # 파라미터 길이만큼 반복 호출
                while repeat_num <= params_len:
                    
                    # 총 페이지 수만큼 반복 호출
                    while page_no <= total_page:
                        
                        # 파라미터 길이만큼 호출 시 while 종료
                        if repeat_num > params_len:
                            break
                        
                        # 재시도 5회 이상 시
                        if retry_num >= 5:
                            # 파라미터 길이 == 1) whlie 종료
                            if params_len == 1:
                                repeat_num += 1
                                break
                            else:  # 파라미터 길이 != 1)
                                # 총 페이지 수만큼 덜 돌았을 때
                                if page_no < total_page:  # 다음 페이지 호출
                                    retry_num = 0
                                    page_no += 1
                                    continue
                                # 총 페이지 수만큼 다 돌고
                                elif page_no == total_page:
                                    # 파라미터 길이만큼 덜 돌았을 때
                                    if repeat_num < params_len:
                                        retry_num = 0
                                        page_no = 1
                                        repeat_num += 1
                                        continue
                                    # 파라미터 길이만큼 다 돌았을 때
                                    else:
                                        repeat_num += 1
                                        break

                        # url 설정
                        # return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"
                        return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, link_se_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"
                        
                        # url 호출
                        response = requests.get(return_url, verify= False)
                        response_code = response.status_code

                        # url 호출 시 메세지 설정
                        header, mode = CallUrlUtil.get_request_message(retry_num, repeat_num, page_no, return_url, total_page, None, header, mode)

                        if response_code == 200:
                            logging.info(f"Status Code: {response_code}")
                            logging.info(f"Raw Response Text: {response.text[:500]}")  # 너무 길면 앞부분만
                            
                            if 'NO_DATA' not in response.text:
                                try:
                                    if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "json":
                                        json_data = response.json()
                                        logging.info("JSON 파싱 성공")
                                    else:  # xml
                                        json_data = XMLtoDict().parse(response.text)
                                        logging.info("XML 파싱 성공")
                                except Exception as e:
                                    logging.error(f"파싱 에러 발생: {str(e)}")
                                    raise e
                        # if response_code == 200 and 'NO_DATA' not in response.text:
                        #     if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "json":
                        #         json_data = response.json()
                        #     else:  # xml
                        #         json_data = XMLtoDict().parse(response.text) 

                            result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                            result_json = result['result_json_array']
                            result_size = len(result_json)
                            
                            # 데이터 존재 시
                            if result_size != 0:
                                retry_num = 0  # 재시도 횟수 초기화
                                if page_no == 1: # 첫 페이지일 때
                                    # 페이징 계산
                                    total_count = int(result['total_count'])
                                    total_page = CallUrlUtil.get_total_page(total_count, result_size)

                                with session.begin() as conn:
                                    # DB 적재
                                    bulk_data = [
                                        {
                                            'baseDate': result['baseDate'],
                                            'baseTime': result['baseTime'],
                                            'category': result['category'],
                                            'nx': result['nx'],
                                            'ny': result['ny'],
                                            'obsrvalue': result['obsrValue'],
                                            'data_crtr_pnttm': th_data_clct_mastr_log.data_crtr_pnttm,
                                            'clct_pnttm': DateUtil.get_ymdhm(),
                                            'clct_log_sn': th_data_clct_mastr_log.clct_log_sn,
                                            'page_no': page_no
                                        } for result in result_json
                                    ]
                                    columns = ', '.join(bulk_data[0].keys())
                                    values = ', '.join([
                                        f"({', '.join(map(lambda x: format_value(x) if x is not None else 'Null', row.values()))})"
                                        for row in bulk_data
                                    ])
                                    delete_stmt = f'''
                                        TRUNCATE TABLE {tn_data_bsc_info.dw_tbl_phys_nm};
                                    '''
                                    conn.execute(delete_stmt)
                                    insert_stmt = f'''
                                        INSERT INTO {tn_data_bsc_info.dw_tbl_phys_nm} ({columns}) VALUES {values};
                                    '''
                                    conn.execute(insert_stmt)

                            # 총 페이지 수 == 1)
                            if total_page == 1:
                                repeat_num += 1
                                break
                            else:
                                if page_no < total_page:
                                    page_no += 1
                                elif page_no == total_page:
                                    if params_len == 1:
                                        repeat_num += 1
                                        break
                                    elif params_len != 1:
                                        if repeat_num < params_len:
                                            page_no = 1
                                            repeat_num += 1
                                        else: repeat_num += 1
                                        break

                        else:
                            logging.info(f"call_url resultmsg::: NO_DATA")
                            retry_num += 1
                            time.sleep(120)
                            continue

                if retry_num >= 5:
                    logging.info(f"call_url ::: {CONST.MSG_CLCT_ERROR_CALL}")
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
                    if now().strftime("%H") == "23":  # 마지막 로그 URL호출 및 CSV생성 실패여도 DW 적재 성공 처리
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_COMP, CONST.MSG_DW_LDADNG_COMP, "n")
                    raise AirflowSkipException()
                else:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP, "n")
            except AirflowSkipException as e:
                raise e
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
                logging.info(f"call_url Exception::: {e}")
                raise e
            
        @task
        def check_loading_result(collect_data_list):
            """
            DW 적재 결과 확인
            params: collect_data_list
            """
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            dw_tbl_phys_nm = tn_data_bsc_info.dw_tbl_phys_nm
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
            
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']

            result_count = 0
            get_count_stmt = f"""SELECT COUNT(data_crtr_pnttm) FROM {dw_tbl_phys_nm} WHERE data_crtr_pnttm = '{data_crtr_pnttm}'"""
            try:
                with session.begin() as conn:
                    result_count = conn.execute(get_count_stmt).first()[0]
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, collect_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    th_data_clct_mastr_log.dw_rcrd_cnt = result_count
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_COMP, CONST.MSG_DW_LDADNG_COMP, "n")
                    logging.info(f"check_loading_result dw_rcrd_cnt::: {result_count}")
            except Exception as e:
                logging.error(f"check_loading_result Exception::: {e}")
                raise e

        file_path = create_directory(collect_data_list)
        file_path >> call_url(collect_data_list, file_path) >> check_loading_result(collect_data_list)
    
    collect_data_list = insert_collect_data_info()
    call_url_process.expand(collect_data_list = collect_data_list)

dag_object = api_to_dw_hour()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024,9,6,13,5,00, tz="Asia/Seoul"),
        # execution_date=datetime(2024,1,30,15,00,00),
        # execution_date=datetime(2024,1,31,14,50,00),
        # execution_date=datetime(2024,1,31,14,55,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )