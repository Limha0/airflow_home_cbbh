import logging
import urllib3
import os

from datetime import datetime as dt
from pendulum import datetime, from_format
from airflow.decorators import dag, task, task_group
from util.common_util import CommonUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from dto.tn_clct_file_info import TnClctFileInfo
from dto.th_data_clct_contact_fail_hstry_log import ThDataClctCallFailrHistLog
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.operators.sftp import SFTPHook
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dag(
    dag_id="sdag_api_to_csv_call_fail_retry",
    schedule="*/30 1-2 2,15,28 * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["api_to_csv"],
)
def api_to_csv_call_fail_retry():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    # SFTPHook 객체
    sftp_hook = SFTPHook(ssh_conn_id='ssh_inner_conn')

    @task
    def select_collect_data_fail_log_sn(**kwargs):
        """
        th_data_clct_contact_fail_hstry_log 테이블에서 수집 실패 대상 정보 clct_log_sn별 조회
        return: fail_log_sn_list
        """
        # 재수집 대상 로그 정보 조회
        select_stmt = get_select_stmt("distinct", kwargs)
        fail_log_sn_list = []
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_stmt).all():
                    th_data_clct_contact_fail_hstry_log = ThDataClctCallFailrHistLog(**dict_row)
                    fail_log_sn_list.append({
                                        "th_data_clct_contact_fail_hstry_log": th_data_clct_contact_fail_hstry_log.as_dict()
                                        })
        except Exception as e:
            logging.error(f"select_collect_data_fail_log_sn Exception::: {e}")
            raise e
        if fail_log_sn_list == []:
            logging.info(f"select_collect_data_fail_log_sn ::: 재수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return fail_log_sn_list

    def get_select_stmt(set_stmt, kwargs):
        """
        select_stmt 설정
        params: set_stmt, kwargs
        return: select_stmt
        """
        run_conf = ""
        if kwargs['dag_run'].conf != {}:
            dtst_cd = kwargs['dag_run'].conf['dtst_cd']
            run_conf = f"AND LOWER(dtst_cd) = '{dtst_cd}'"
        
        distinct_stmt = ""
        where_stmt = ""
        if set_stmt == "distinct":
            distinct_stmt = "DISTINCT ON (clct_log_sn)"
        else:
            where_stmt = f"AND clct_log_sn = {set_stmt} ORDER BY sn"
        
        select_stmt = f'''
                    SELECT {distinct_stmt} *
                    FROM th_data_clct_contact_fail_hstry_log
                    WHERE 1=1
                        AND LOWER(stts_cd) = '{CONST.STTS_ERROR}'
                        {run_conf}
                        {where_stmt}
                    '''
        return select_stmt

    @task_group(group_id='call_url_process')
    def call_url_process(fail_log_sn_list):
        from util.file_util import FileUtil

        @task
        def select_collect_data_fail_info(fail_log_sn_list, **kwargs):
            """
            th_data_clct_contact_fail_hstry_log 테이블에서 재수집 대상 정보 조회
            return: fail_data_lists
            """
            th_data_clct_contact_fail_hstry_log = fail_log_sn_list['th_data_clct_contact_fail_hstry_log']
            info_for_update = mastr_log_info_for_update(fail_log_sn_list, kwargs)
            tn_data_bsc_info = TnDataBscInfo(**info_for_update['tn_data_bsc_info'])
            th_data_clct_mastr_log = ThDataClctMastrLog(**info_for_update['th_data_clct_mastr_log'])
            tn_clct_file_info = TnClctFileInfo(**info_for_update['tn_clct_file_info'])
            log_full_file_path = info_for_update['log_full_file_path']
            th_data_clct_mastr_log.reclect_flfmt_nmtm += 1  # 재수집 수행 횟수 증가
            with session.begin() as conn:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CNTN, CONST.STTS_WORK, CONST.MSG_CNTN_WORK, "y")

            # 재수집 대상 로그 정보 조회
            select_stmt = get_select_stmt(th_data_clct_contact_fail_hstry_log['clct_log_sn'], kwargs)
            fail_data_lists = []
            try:
                with session.begin() as conn:
                    for dict_row in conn.execute(select_stmt).all():
                        th_data_clct_contact_fail_hstry_log = ThDataClctCallFailrHistLog(**dict_row)

                        # 재수집 대상 기본 정보 조회
                        select_bsc_info_stmt = f'''
                                                SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                                                FROM tn_data_bsc_info
                                                WHERE 1=1
                                                    AND LOWER(dtst_cd) = '{th_data_clct_contact_fail_hstry_log.dtst_cd}'
                                                '''
                        dict_row_info = conn.execute(select_bsc_info_stmt).first()
                        tn_data_bsc_info = TnDataBscInfo(**dict_row_info)

                        th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_contact_fail_hstry_log.clct_log_sn)

                        file_name = th_data_clct_mastr_log.clct_data_nm + "_" + th_data_clct_mastr_log.data_crtr_pnttm
                        tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                        # 수집로그파일 경로 set
                        log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), kwargs)
                        
                        fail_data_lists.append({
                                            "th_data_clct_contact_fail_hstry_log": th_data_clct_contact_fail_hstry_log.as_dict()
                                            , "tn_data_bsc_info": tn_data_bsc_info.as_dict()
                                            , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                            , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                            , "log_full_file_path" : log_full_file_path
                                            })
            except Exception as e:
                logging.error(f"select_collect_data_fail_info Exception::: {e}")
                raise e
            return fail_data_lists

        @task
        def call_url(fail_data_lists, **kwargs):
            """
            clct_log_sn별 URL 호출하여 csv 파일 생성
            params: fail_data_lists
            return: file_size
            """
            import requests
            import time
            from util.call_url_util import CallUrlUtil
            from xml_to_dict import XMLtoDict
            
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            for fail_data_list in fail_data_lists:
                try:
                    th_data_clct_contact_fail_hstry_log = ThDataClctCallFailrHistLog(**fail_data_list['th_data_clct_contact_fail_hstry_log'])
                    th_data_clct_mastr_log = ThDataClctMastrLog(**fail_data_list['th_data_clct_mastr_log'])
                    tn_data_bsc_info = TnDataBscInfo(**fail_data_list['tn_data_bsc_info'])

                    dtst_cd = th_data_clct_contact_fail_hstry_log.dtst_cd.lower()
                    pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
                    pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()
                    # base_url = return_url = tn_data_bsc_info.link_data_clct_url
                    return_url = th_data_clct_contact_fail_hstry_log.clct_fail_url

                    # 파라미터 설정
                    params = th_data_clct_contact_fail_hstry_log.estn_field_one

                    retry_num = 0  # 데이터 없을 시 재시도 횟수
                    page_no = th_data_clct_contact_fail_hstry_log.clct_pgng_no
                    
                    header = False   # 파일 헤더 모드
                    mode = "w"  # 파일 쓰기 모드 overwrite
                    source_mode = "w"
                    file_name = th_data_clct_mastr_log.clct_data_nm + "_" + th_data_clct_mastr_log.data_crtr_pnttm + "." + tn_data_bsc_info.link_file_extn  # csv 파일명
                    source_file_name = th_data_clct_mastr_log.clct_data_nm + "_retry." + tn_data_bsc_info.pvdr_sou_data_pvsn_stle  # 원천 파일명
                    file_path = th_data_clct_contact_fail_hstry_log.estn_field_two
                    full_file_path = root_collect_file_path + file_path
                    full_file_name = full_file_path + file_name
                    link_file_sprtr = tn_data_bsc_info.link_file_sprtr
                    row_count = 0  # 행 개수

                    while True:
                        # 재시도 5회 이상 시 whlie 종료
                        if retry_num >= 5:
                            break

                        # url 설정
                        # return_url = base_url + { "data19" : f"{page_no}&bjdongCd=" }.get(dtst_cd) + params
                        
                        # url 호출
                        response = requests.get(return_url, verify= False)                    
                        response_code = response.status_code

                        # url 호출 시 메세지 설정
                        if retry_num == 0:  # 첫 호출
                            if os.path.exists(full_file_name):  # 기존 파일 존재 시 파일쓰기 모드 append
                                mode = "a"
                            logging.info(f"호출 url: {return_url}")
                        else:  # 재호출
                            logging.info(f"호출 결과 에러, url: {return_url} 로 재호출, 재시도 횟수: {retry_num}")

                        if response_code == 200:
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "json" and 'OpenAPI_ServiceResponse' not in response.text:  # 공공데이터포털 - HTTP 에러 제외
                                json_data = response.json()
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "xml" or 'OpenAPI_ServiceResponse' in response.text:  # 공공데이터포털 - HTTP 에러 시 xml 형태
                                json_data = XMLtoDict().parse(response.text)

                            if os.path.exists(full_file_path + source_file_name):  # 기존 원천 파일 존재 시 파일쓰기 모드 append
                                source_mode = "a"

                            # 원천 데이터 저장
                            CallUrlUtil.create_source_file(json_data, source_file_name, full_file_path, source_mode)

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
                                row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                                if row_count == 0:
                                    header = True
                                    mode = "w"

                                # csv 파일 생성
                                CallUrlUtil.create_csv_file(link_file_sprtr, th_data_clct_mastr_log.data_crtr_pnttm, th_data_clct_mastr_log.clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no)

                            # 데이터 결과 없을 경우
                            else:
                                logging.info(f"{CONST.MSG_CLCT_COMP_NO_DATA}")

                            row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                            if row_count != 0:
                                logging.info(f"현재까지 파일 내 행 개수: {row_count}")
                            break
                        else:
                            logging.info(f"call_url response_code::: {response_code}")
                            retry_num += 1
                            time.sleep(5)
                            continue
                    
                    with session.begin() as conn:
                        th_data_clct_contact_fail_hstry_log = conn.get(ThDataClctCallFailrHistLog, th_data_clct_contact_fail_hstry_log.sn)

                        # th_data_clct_contact_fail_hstry_log 업데이트
                        if result_size == 0 and retry_num < 5:  # 원천데이터 없음
                            CallUrlUtil.update_fail_history_log(th_data_clct_contact_fail_hstry_log, session, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA)
                        elif retry_num >= 5:  # URL호출 실패
                            CallUrlUtil.update_fail_history_log(th_data_clct_contact_fail_hstry_log, session, CONST.STTS_ERROR,CONST.MSG_CLCT_ERROR_CALL)
                        else:  # URL호출 및 CSV생성 성공
                            CallUrlUtil.update_fail_history_log(th_data_clct_contact_fail_hstry_log, session, CONST.STTS_COMP, CONST.MSG_CLCT_COMP)
                except Exception as e:
                    CallUrlUtil.update_fail_history_log(th_data_clct_contact_fail_hstry_log, session, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL)
                    logging.error(f"call_url Exception::: {e}")
        
        def mastr_log_info_for_update(fail_log_sn_list, kwargs):
            """
            th_data_clct_mastr_log 업데이트용 info
            params: fail_log_sn_list, kwargs
            return: info_for_update
            """
            th_data_clct_contact_fail_hstry_log = fail_log_sn_list['th_data_clct_contact_fail_hstry_log']
            with session.begin() as conn:
                th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_contact_fail_hstry_log['clct_log_sn'])
                dtst_cd = th_data_clct_mastr_log.dtst_cd
                file_name = th_data_clct_mastr_log.clct_data_nm + "_" + th_data_clct_mastr_log.data_crtr_pnttm

                # 기본 정보 조회
                select_bsc_info_stmt = f'''
                                    SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                                    FROM tn_data_bsc_info
                                    WHERE 1=1
                                        AND LOWER(clct_yn) = 'y'
                                        AND LOWER(link_yn) = 'y'
                                        AND LOWER(dtst_cd) = '{dtst_cd}'
                                    '''
                dict_row_info = conn.execute(select_bsc_info_stmt).first()
                tn_data_bsc_info = TnDataBscInfo(**dict_row_info)

                # 수집파일정보 set
                tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                # 수집로그파일 경로 set
                log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), kwargs)
            return {
                    "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                    , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                    , "tn_clct_file_info": tn_clct_file_info.as_dict()
                    , "log_full_file_path" : log_full_file_path
                    , "file_name" : file_name
                }
        
        @task
        def mastr_log_update(fail_log_sn_list, **kwargs):
            """
            clct_log_sn별 URL호출 결과 th_data_clct_mastr_log에 업데이트
            params: fail_log_sn_list
            return: success_data_list
            """
            from util.call_url_util import CallUrlUtil

            th_data_clct_contact_fail_hstry_log = fail_log_sn_list['th_data_clct_contact_fail_hstry_log']
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            success_data_list = []

            info_for_update = mastr_log_info_for_update(fail_log_sn_list, kwargs)
            tn_data_bsc_info = TnDataBscInfo(**info_for_update['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**info_for_update['tn_clct_file_info'])
            log_full_file_path = info_for_update['log_full_file_path']
            file_name = info_for_update['file_name'] + "." + tn_data_bsc_info.link_file_extn  # csv 파일명

            with session.begin() as conn:
                th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_contact_fail_hstry_log['clct_log_sn'])

                try:
                    file_path = th_data_clct_contact_fail_hstry_log['estn_field_two']
                    full_file_path = root_collect_file_path + file_path
                    full_file_name = full_file_path + file_name
                    
                    link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
                    row_count = FileUtil.check_csv_length(tn_data_bsc_info.link_file_sprtr, full_file_name)  # 행 개수 확인

                    # 파일 사이즈 확인
                    if os.path.exists(full_file_name):
                        file_size = os.path.getsize(full_file_name)
                    logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")
                    
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)

                    # 실패 로그 개수 확인
                    fail_count = CallUrlUtil.get_fail_data_count(th_data_clct_mastr_log.clct_log_sn, session)

                    # th_data_clct_mastr_log 업데이트
                    if row_count == 0 and fail_count == 0:  # 원천데이터 없음
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "y")
                        raise AirflowSkipException()
                    elif fail_count != 0:  # URL호출 실패
                        logging.info(f"call_url ::: {CONST.MSG_CLCT_ERROR_CALL}")
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "y")
                        raise AirflowSkipException()
                    else:  # URL호출 및 CSV생성 성공
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP, "y")
                        success_data_list.append({
                            "tn_data_bsc_info": tn_data_bsc_info.as_dict()
                            , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                            , "tn_clct_file_info": tn_clct_file_info.as_dict()
                            , "log_full_file_path": log_full_file_path
                            , "file_path": file_path
                        })
                except AirflowSkipException as e:
                    raise e
                except Exception as e:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "y")
                    logging.error(f"mastr_log_update Exception::: {e}")
                    raise e
            return success_data_list[0]
        
        @task
        def encrypt_zip_file(success_data_list, **kwargs):
            """
            파일 압축 및 암호화
            params: fail_data_list
            return: encrypt_file
            """
            tn_data_bsc_info = TnDataBscInfo(**success_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**success_data_list['tn_clct_file_info'])
            log_full_file_path = success_data_list['log_full_file_path']
            file_path = success_data_list['file_path']

            try:
                with session.begin() as conn:
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, success_data_list['th_data_clct_mastr_log']['clct_log_sn'])
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
                logging.error(f"encrypt_zip_file Exception::: {e}")
                raise e
            return {
                    "file_path" : file_path,
                    "encrypt_file" : encrypt_file
                    }

        @task
        def put_file_sftp(success_data_list, encrypt_file_path, **kwargs):
            """
            원격지 서버로 sftp 파일전송
            params: fail_data_list, encrypt_file_path
            """
            file_path = encrypt_file_path['file_path']
            encrypt_file = encrypt_file_path['encrypt_file']
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            full_file_path = root_collect_file_path + file_path

            local_filepath = full_file_path + encrypt_file
            remote_filepath = kwargs['var']['value'].final_file_path + file_path

            tn_clct_file_info = TnClctFileInfo(**success_data_list['tn_clct_file_info'])
            log_full_file_path = success_data_list['log_full_file_path']
            
            with session.begin() as conn:
                th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, success_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                try:
                    if not sftp_hook.path_exists(remote_filepath):
                        sftp_hook.create_directory(remote_filepath)
                    sftp_hook.store_file(remote_filepath + encrypt_file, local_filepath)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_EXT, "y")
                except Exception as e:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_TRANS_EXT, "y")
                    logging.error(f"put_file_sftp Exception::: {e}")
                    raise e

        fail_data_lists = select_collect_data_fail_info(fail_log_sn_list)
        success_data_list = mastr_log_update(fail_log_sn_list)
        encrypt_file_path = encrypt_zip_file(success_data_list)
        call_url(fail_data_lists) >> success_data_list >> encrypt_file_path >> put_file_sftp(success_data_list, encrypt_file_path)
    
    fail_log_sn_list = select_collect_data_fail_log_sn()
    call_url_process.expand(fail_log_sn_list = fail_log_sn_list)

dag_object = api_to_csv_call_fail_retry()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    # dtst_cd = "data19"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2023,10,1,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )