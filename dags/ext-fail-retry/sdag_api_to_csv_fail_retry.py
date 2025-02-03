import logging
import urllib3

from datetime import datetime as dt
from pendulum import datetime, from_format, now
from airflow.decorators import dag, task, task_group
from util.common_util import CommonUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.operators.sftp import SFTPHook
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="sdag_api_to_csv_fail_retry",
    schedule="*/30 1-2 2,3,15,28 * *",
    start_date=datetime(2023, 10, 30, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["api_to_csv_retry", "month", "ext"],
)
def api_to_csv_fail_retry():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    # SFTPHook 객체
    sftp_hook = SFTPHook(ssh_conn_id='ssh_inner_conn')

    @task
    def select_collect_data_fail_info(**kwargs):
        """
        th_data_clct_mastr_log 테이블에서 재수집 대상 로그 정보 조회, tn_data_bsc_info 테이블에서 재수집 대상 기본 정보 조회
        return: tn_data_bsc_info
        """
        run_conf = ""
        dtst_cd = ""
        if kwargs['dag_run'].conf != {}:
            dtst_cd = kwargs['dag_run'].conf['dtst_dtl_cd']
            run_conf = f"AND LOWER(a.dtst_dtl_cd) = '{dtst_cd}'"

        # 재수집 대상 로그 정보 조회
        select_log_info_stmt = f'''
                            SELECT b.*
                            FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                            WHERE 1=1
                                AND a.dtst_cd = b.dtst_cd
                                AND LOWER(clct_yn) = 'y'
                                AND LOWER(link_yn) = 'y'
                                AND LOWER(link_clct_mthd_dtl_cd) = 'open_api'
                                AND link_ntwk_otsd_insd_se = '외부'
                                AND LOWER(a.dtst_cd) NOT in ('data5', 'data6') -- 국가교통정보센터_공사_사고정보, 국가교통정보센터_CCTV_영상정보
                                AND LOWER(pvdr_site_cd) != 'ps00010' -- 국가통계포털 제외
                                AND LOWER(pvdr_inst_cd) NOT IN ('pi00002', 'pi00011') -- 경기도시장상권진흥원, 카페_게시글, 양주시_뉴스_기사, 네이버블로그 제외
                                AND LOWER(a.dtst_cd) not in ('data4', 'data799', 'data31', 'data852', 'data855', 'data856') -- 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 기상청_단기예보, 실시간_측정정보_조회, 측정소_측정정보_조회 제외
                                AND LOWER(pvdr_sou_data_pvsn_stle) != 'zip'
                                AND LOWER(b.step_se_cd) NOT IN ('{CONST.STEP_FILE_STRGE_SEND}', '{CONST.STEP_DW_LDADNG}') -- 하둡전송단계, DW 적재단계 제외
                                AND COALESCE(stts_msg, '') NOT IN ('{CONST.MSG_CLCT_COMP_NO_DATA}', '{CONST.MSG_CLCT_WORK}') -- 원천데이터 없음, url 호출 중인 것 제외
                                AND NOT (LOWER(b.step_se_cd) = '{CONST.STEP_FILE_INSD_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') -- 내부파일전송 성공 제외
                                AND b.clct_log_sn NOT IN (
                                    SELECT clct_log_sn
                                    FROM th_data_clct_contact_fail_hstry_log
                                    WHERE LOWER(stts_cd) != '{CONST.STTS_COMP}'
                                ) -- th_data_clct_contact_fail_hstry_log 에 해당하는 로그는 제외
                                {run_conf}
                            ORDER BY b.clct_log_sn
                            '''
        logging.info(f"select_collect_data_fail_info !!!!!::: {select_log_info_stmt}")
        # try:
        #     TriggerDagRunOperator(
        #         trigger_dag_id='sdag_api_to_csv_call_fail_retry',
        #         task_id='sdag_api_to_csv_call_fail_retry',
        #         execution_date=now(),
        #         wait_for_completion=True,
        #         poke_interval=30,
        #         allowed_states=["success"],
        #         conf={"dtst_cd": dtst_cd}
        #     ).execute(kwargs)
        #     collect_data_list = CommonUtil.set_fail_info(session, select_log_info_stmt, kwargs)
        # except Exception as e:
        #     logging.info(f"select_collect_data_fail_info Exception::: {e}")
        #     raise e
        # if collect_data_list == []:
        #     logging.info(f"select_collect_data_fail_info ::: 재수집 대상없음 프로그램 종료")
        #     raise AirflowSkipException()
        # return collect_data_list
        try:
            collect_data_list = CommonUtil.set_fail_info(session, select_log_info_stmt, kwargs)
        except Exception as e:
            logging.info(f"select_collect_data_fail_info Exception::: {e}")
            raise e
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 재수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return collect_data_list
    
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
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), root_collect_file_path, "y")
            return file_path
        
        @task
        def call_url(collect_data_list, file_path, **kwargs):
            """
            조건별 URL 설정 및 호출하여 csv 파일 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
            return: file_size
            """
            import requests
            import os
            import time
            from util.call_url_util import CallUrlUtil
            from xml_to_dict import XMLtoDict

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path

            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            link_se_cd = tn_data_bsc_info.link_se_cd.lower()
            pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
            pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()
            base_url = return_url = tn_data_bsc_info.link_data_clct_url

            # 파라미터 및 파라미터 길이 설정
            data_crtr_pnttm_str = th_data_clct_mastr_log.data_crtr_pnttm
            if len(data_crtr_pnttm_str) == 4:
                data_crtr_pnttm = from_format(data_crtr_pnttm_str,'YYYY')
            if len(data_crtr_pnttm_str) == 6:
                data_crtr_pnttm = from_format(data_crtr_pnttm_str,'YYYYMM')
            if len(data_crtr_pnttm_str) == 8:
                data_crtr_pnttm = from_format(data_crtr_pnttm_str,'YYYYMMDD')
            data_interval_start = data_crtr_pnttm  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
            data_interval_end = from_format(th_data_clct_mastr_log.clct_ymd,'YYYYMMDD')  # 실제 실행하는 날짜를 KST 로 설정
            params_dict, params_len = CallUrlUtil.set_params(tn_data_bsc_info, session, data_interval_start, data_interval_end, kwargs)

            retry_num = 0  # 데이터 없을 시 재시도 횟수
            repeat_num = 1  # 파라미터 길이만큼 반복 호출 횟수
            page_no = 1  # 현재 페이지
            total_page = 1  # 총 페이지 수
            
            header = True   # 파일 헤더 모드
            mode = "w"  # 파일 쓰기 모드 overwrite
            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
            source_file_name = tn_clct_file_info.insd_file_nm + "." + tn_data_bsc_info.pvdr_sou_data_pvsn_stle  # 원천 파일명
            full_file_path = root_collect_file_path + file_path
            full_file_name = full_file_path + file_name
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr
            file_size = 0  # 파일 사이즈
            row_count = 0  # 행 개수

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
                                # th_data_clct_contact_fail_hstry_log 에 입력
                                CallUrlUtil.insert_fail_history_log(th_data_clct_mastr_log, return_url, file_path, session, params_dict['param_list'][repeat_num - 1], page_no)

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
                        return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, link_se_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"
                        # return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"
                        
                        # url 호출
                        response = requests.get(return_url, verify= False)
                        response_code = response.status_code

                        # url 호출 시 메세지 설정
                        header, mode = CallUrlUtil.get_request_message(retry_num, repeat_num, page_no, return_url, total_page, full_file_name, header, mode)
                        
                        if response_code == 200:
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "json" and 'OpenAPI_ServiceResponse' not in response.text and '제공 가능한 데이터가 없습니다' not in response.text:  # 공공데이터포털 - HTTP 에러 제외
                                json_data = response.json()
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "xml" or 'OpenAPI_ServiceResponse' in response.text or '제공 가능한 데이터가 없습니다' in response.text:  # 공공데이터포털, 지역별 독서량_독서율(20240411 이후 변경) - HTTP 에러 시 xml 형태
                                json_data = XMLtoDict().parse(response.text)

                            # 원천 데이터 저장
                            CallUrlUtil.create_source_file(json_data, source_file_name, full_file_path, mode)

                            # 공공데이터포털 - HTTP 에러 시
                            if 'OpenAPI_ServiceResponse' in response.text:
                                retry_num += 1
                                continue

                            result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                            result_json = result['result_json_array']
                            result_size = len(result_json)

                             # 데이터 구분 컬럼, 값 추가
                            add_column = tn_data_bsc_info.data_se_col_two
                            add_column_dict = {}
                            if pvdr_inst_cd == "pi00012" and dtst_cd not in {"data787", "data788"}:  #TAAS
                                add_column_dict = {add_column : params}
                            if dtst_cd in {"data656", "data667"}:
                                add_column_dict = {add_column : "세입"}
                            if dtst_cd in {"data657", "data668"}:
                                add_column_dict = {add_column : "세출"}
                            
                            # 컬럼 존재하지않는 경우 예외 처리
                            get_data_column_stmt = f"""
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_name = '{tn_data_bsc_info.dw_tbl_phys_nm}'
                                AND column_name NOT IN ('data_crtr_pnttm','clct_sn','clct_pnttm','clct_log_sn','page_no')
                                ORDER BY ordinal_position
                            """
                            with session.begin() as conn:
                                dw_column_dict = []  # DW 컬럼명
                                for dict_row in conn.execute(get_data_column_stmt).all():
                                    dw_column_dict.append(dict_row[0])

                            for dict_value in result_json:
                                dict_value.update(add_column_dict)
                                lowercase_keys = {key.lower(): value for key, value in dict_value.items()}
                                for missing_column in dw_column_dict:
                                    if missing_column not in lowercase_keys.keys():
                                        add_column_dict.update({missing_column: None})
                                dict_value.update(add_column_dict)
                            
                            # 데이터 존재 시
                            if result_size != 0:
                                retry_num = 0  # 재시도 횟수 초기화
                                if page_no == 1: # 첫 페이지일 때
                                    # 페이징 계산
                                    total_count = int(result['total_count'])
                                    total_page = CallUrlUtil.get_total_page(total_count, result_size)

                                row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                                if row_count == 0:
                                    header = True
                                    mode = "w"

                                # csv 파일 생성
                                CallUrlUtil.create_csv_file(link_file_sprtr, th_data_clct_mastr_log.data_crtr_pnttm, th_data_clct_mastr_log.clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no)

                            # 데이터 결과 없을 경우
                            else:
                                # 가변 파라미터 변경 후 재호출
                                if dtst_cd != 'data785' and params_len == 1 and params_dict != {} and retry_num < 4:
                                    params = params_dict['params']
                                    retry_num += 1
                                    if len(str(params)) == 4:  # yyyy
                                        params_dict['params'] -= 1  # year -= 1
                                    if len(str(params)) == 6:  # yyyymm
                                        params_dict['params'] = from_format(params, 'YYYYMM').add(months=-1).strftime("%Y%m")  # 이전 달 호출
                                    else:
                                        break
                                    continue
                                # if dtst_cd == 'data785':  # 이달의 키워드 예외
                                #     retry_num = 5
                                #     break

                            row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                            if row_count != 0:
                                logging.info(f"현재까지 파일 내 행 개수: {row_count}")

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
                            logging.info(f"call_url response_code::: {response_code}")
                            retry_num += 1
                            time.sleep(5)
                            continue

                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    file_size = os.path.getsize(full_file_name)
                logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")

                # 실패 로그 개수 확인
                fail_count = CallUrlUtil.get_fail_data_count(th_data_clct_mastr_log.clct_log_sn, session)
                
                if row_count == 0 and fail_count == 0 and retry_num < 5:
                    if dtst_cd in {'data33'}:  # 대기오염_국가측정망_월평균_측정정보
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, '원천 데이터 없음', "n")
                    else:
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "n")
                    raise AirflowSkipException()
                elif fail_count != 0 or retry_num >= 5:
                    logging.info(f"call_url ::: {CONST.MSG_CLCT_ERROR_CALL}")
                    if dtst_cd in {'data785','data786'}:
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, '원천 데이터 없음', "n")
                    else:
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
                        raise AirflowSkipException()
                else:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP, "y")
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
            except AirflowSkipException as e:
                raise e
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "y")
                logging.info(f"call_url Exception::: {e}")
                raise e
        
        @task
        def encrypt_zip_file(collect_data_list, file_path, **kwargs):
            """
            파일 압축 및 암호화
            params: collect_data_list, file_path
            return: encrypt_file
            """
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']
            try:
                with session.begin() as conn:
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, collect_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_WORK, CONST.MSG_FILE_INSD_SEND_WORK, "y")

                    pvdr_site_nm = tn_data_bsc_info.pvdr_site_nm
                    link_file_extn = tn_data_bsc_info.link_file_extn
                    pvdr_sou_data_pvsn_stle = tn_data_bsc_info.pvdr_sou_data_pvsn_stle
                    encrypt_key = kwargs['var']['value'].encrypt_key
                    root_collect_file_path = kwargs['var']['value'].root_collect_file_path
                    full_file_path = root_collect_file_path + file_path

                    FileUtil.zip_file(full_file_path, pvdr_site_nm, link_file_extn, pvdr_sou_data_pvsn_stle)
                    encrypt_file = FileUtil.encrypt_file(full_file_path, pvdr_site_nm, encrypt_key, pvdr_sou_data_pvsn_stle)
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_FILE, "y")
                logging.info(f"encrypt_zip_file Exception::: {e}")
                raise e
            return {
                    "file_path" : file_path,
                    "encrypt_file" : encrypt_file
                    }

        @task
        def put_file_sftp(collect_data_list, encrypt_file_path, **kwargs):
            """
            원격지 서버로 sftp 파일전송
            params: collect_data_list, encrypt_file_path
            """
            file_path = encrypt_file_path['file_path']
            encrypt_file = encrypt_file_path['encrypt_file']
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            full_file_path = root_collect_file_path + file_path

            local_filepath = full_file_path + encrypt_file
            remote_filepath = kwargs['var']['value'].final_file_path + file_path
            
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']

            with session.begin() as conn:
                th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, collect_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                try:
                    if not sftp_hook.path_exists(remote_filepath):
                        sftp_hook.create_directory(remote_filepath)
                    sftp_hook.store_file(remote_filepath + encrypt_file, local_filepath)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_EXT, "y")
                except Exception as e:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_TRANS_EXT, "y")
                    logging.info(f"put_file_sftp Exception::: {e}")
                    raise e

        file_path = create_directory(collect_data_list)
        encrypt_file_path = encrypt_zip_file(collect_data_list, file_path)
        file_path >> call_url(collect_data_list, file_path) >> encrypt_file_path >> put_file_sftp(collect_data_list, encrypt_file_path)
    
    collect_data_list = select_collect_data_fail_info()
    call_url_process.expand(collect_data_list = collect_data_list)

dag_object = api_to_csv_fail_retry()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""
    # dtst_cd = "data678"  # test

    dag_object.test(
        execution_date=datetime(2023,12,31,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )