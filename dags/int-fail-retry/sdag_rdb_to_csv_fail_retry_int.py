import logging

from datetime import datetime as dt
from pendulum import datetime, from_format
from airflow.decorators import dag, task, task_group
from util.common_util import CommonUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from dto.tn_db_cntn_info import TnDBCntnInfo
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException

@dag(
    dag_id="sdag_rdb_to_csv_fail_retry_int",
    schedule="*/30 1-2 2,15,28 * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["db_to_csv_retry", "month", "int"],
)
def rdb_to_csv_fail_retry():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')
    
    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    @task
    def select_collect_data_fail_info(**kwargs):
        """
        th_data_clct_mastr_log 테이블에서 재수집 대상 로그 정보 조회, tn_data_bsc_info 테이블에서 재수집 대상 기본 정보 조회
        return: collect_data_list
        """
        run_conf = ""
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
                                AND LOWER(link_clct_mthd_dtl_cd) = 'rdb'
                                AND link_ntwk_otsd_insd_se = '내부'
                                AND LOWER(link_clct_cycle_cd) = 'month' --bis
                                AND LOWER(b.step_se_cd) NOT IN ('{CONST.STEP_FILE_STRGE_SEND}', '{CONST.STEP_DW_LDADNG}') -- 스토리지파일전송단계, DW 적재단계 제외
                                AND COALESCE(stts_msg, '') != '{CONST.MSG_CLCT_COMP_NO_DATA}' -- 원천데이터 없음 제외
                                AND NOT (LOWER(b.step_se_cd) = '{CONST.STEP_FILE_INSD_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') -- 내부파일전송 성공 제외
                                {run_conf}
                            ORDER BY b.clct_log_sn
                            '''
        logging.info(f"select_collect_data_fail_info !!!!!::: {select_log_info_stmt}")
        try:
            collect_data_list = CommonUtil.set_fail_info(session, select_log_info_stmt, kwargs)
        except Exception as e:
            logging.info(f"select_collect_data_fail_info Exception::: {e}")
            raise e
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 재수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return collect_data_list

    @task_group(group_id='db_to_csv_fail_retry_process')
    def db_to_csv_process(collect_data_list):
        import os
        import pandas as pd
        
        @task
        def get_db_connection_url(collect_data_list):
            """
            tn_db_cntn_info 테이블에서 연계DB접속정보 조회
            연계 DB 연결 URL 생성
            params: tn_data_bsc_info
            return: tn_db_cntn_info, connection_url
            """
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']

            # 연계 DB 접속 정보 조회
            link_db_id = tn_data_bsc_info.link_db_id
            select_database_info_stmt = f'''
                                        SELECT
                                            link_db_id, pvdr_sys_nm, db_knd_cd, sid, driver_class_nm
                                            ,(SELECT fn_decrypt(ip) FROM tn_db_cntn_info WHERE link_db_id = a.link_db_id) ip
                                            ,(SELECT fn_decrypt(port) FROM tn_db_cntn_info WHERE link_db_id = a.link_db_id) port
                                            ,(SELECT fn_decrypt(user_id) FROM tn_db_cntn_info WHERE link_db_id = a.link_db_id) user_id
                                            ,(SELECT fn_decrypt(pswd) FROM tn_db_cntn_info WHERE link_db_id = a.link_db_id) pswd
                                        FROM tn_db_cntn_info a
                                        WHERE link_db_id = '{link_db_id}'
                                        '''
            try:
                with engine.connect() as conn:
                    for dict_row in conn.execute(select_database_info_stmt).mappings():
                        tn_db_cntn_info = TnDBCntnInfo(**dict_row)
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CNTN_ERROR, "y")
                logging.info(f"get_db_connection_url Exception::: {e}")
                raise e

            # 연계 DB 연결 URL
            db_knd_cd = tn_db_cntn_info.db_knd_cd.lower()
            ip = tn_db_cntn_info.ip
            port = tn_db_cntn_info.port
            sid = tn_db_cntn_info.sid

            # connection url 생성
            if db_knd_cd in {'mysql', 'mariadb'}:  # MySQL, MariaDB
                connection_url = f'jdbc:{db_knd_cd}://{ip}:{port}/{sid}'
            if db_knd_cd in {'oracle', 'tibero'}:  # oracle, tibero
                connection_url = f'jdbc:{db_knd_cd}:thin:@{ip}:{port}:{sid}'
            logging.info("get_db_connection_url connection_url::: {}".format(connection_url))
            return {
                    'tn_db_cntn_info' : tn_db_cntn_info.as_dict(),
                    'connection_url' : connection_url
                    }

        @task
        def create_directory(collect_data_list, **kwargs):
            """
            수집 파일 경로 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info
            return: file_path: tn_clct_file_info 테이블에 저장할 파일 경로
            """
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            final_file_path = kwargs['var']['value'].final_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), final_file_path, "y")
            return file_path

        @task
        def create_csv_file(collect_data_list, db_connection_url_result, file_path, **kwargs):
            """
            연계 DB SQL 작성 및 csv 파일 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, db_connection_url_result, file_path
            return: full_file_name 생성된 csv 파일
            """
            import jaydebeapi as jp
            from util.date_custom_util import DateUtil
            from util.file_util import FileUtil

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']
            final_file_path = kwargs['var']['value'].final_file_path
            full_file_path = final_file_path + file_path

            # 연계 DB SQL문
            link_tbl_phys_nm = tn_data_bsc_info.link_tbl_phys_nm
            data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
            dw_load_mthd_cd = tn_data_bsc_info.dw_load_mthd_cd.lower()
            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr

            select_db_stmt = f'''SELECT a.*'''
            
            dtst_cd = tn_data_bsc_info.dtst_cd
            if dtst_cd == "data1046": #버스정류소현황
                select_db_stmt = f'''SELECT busstopid,busstopname,wincid,gpsx,gpsy'''
            elif  dtst_cd == "data1047": #ROUTELINK정보
                select_db_stmt = f'''SELECT buslineno,buslineid,busdirectcd,linkorder,linkid'''
            elif  dtst_cd == "data1048": #경유버스정류소현황
                select_db_stmt = f'''SELECT buslineno,busdirectcd,busstoporder,busstopname,busstopid,buslineid'''
            elif  dtst_cd == "data1049": #버스노선현황
                select_db_stmt = f'''SELECT buslineno,rte_stts,rte_type,startbusstopid,endbusstopid'''
            elif  dtst_cd == "data1052": #강우량
                select_db_stmt = f'''SELECT NUM,N_TIME,H01,H02,H03,H04,H05,H06,H07,H08,H09,H10
                                    ,H11,H12,H13,H14,H15,H16,H17,H18,H19,H20,H21,H22,H23,H24,N_SUM
                                    '''

            select_db_stmt += f"""
                                ,'{data_crtr_pnttm}' data_crtr_pnttm
                                ,'{DateUtil.get_ymdhm()}' clct_pnttm
                                ,'{th_data_clct_mastr_log.clct_log_sn}' clct_log_sn
                            FROM {link_tbl_phys_nm} a """
            
            if dw_load_mthd_cd == '3month_change':
                select_db_stmt += " WHERE to_date(indt, 'YYYYMMDDHH24MISS') BETWEEN add_months(trunc(sysdate, 'mm'), -3) AND sysdate"
            logging.info(f"create_csv_file select_db_stmt::: {' '.join(select_db_stmt.split())}")

            tn_db_cntn_info = TnDBCntnInfo(**db_connection_url_result['tn_db_cntn_info'])

            user_id = tn_db_cntn_info.user_id
            pswd = tn_db_cntn_info.pswd
            connection_url = db_connection_url_result['connection_url']
            jars = [f"{os.environ['PYTHONPATH']}/jars/mysql-connector-java-5.1.49.jar", f"{os.environ['PYTHONPATH']}/jars/mariadb-java-client-3.1.4.jar", f"{os.environ['PYTHONPATH']}/jars/ojdbc8.jar"]

            # engine 생성
            try:
                db_engine = jp.connect(f'{tn_db_cntn_info.driver_class_nm}', connection_url, [f'{user_id}', f'{pswd}'], jars)

                # csv 파일 생성
                os.chdir(full_file_path)
                file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
                df = pd.read_sql_query(select_db_stmt, db_engine)
                df = df.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis = 0)  # 개행문자 제거, string 양 끝 공백 제거
                df.index += 1
                df.to_csv(file_name, sep = link_file_sprtr, header = True, index_label= "clct_sn", encoding='utf-8-sig')
                full_file_name = full_file_path + file_name

                # if dtst_cd == "data803":
                #     # csv 한글 헤더를 DW 영문 컬럼명으로 변경
                #     get_data_column_stmt = f"""
                #                 SELECT column_name
                #                 FROM information_schema.columns
                #                 WHERE table_name = '{tn_data_bsc_info.dw_tbl_phys_nm}'
                #                 ORDER BY ordinal_position
                #             """
                #     with session.begin() as conn:
                #         dw_column_dict = []  # DW 컬럼명
                #         for dict_row in conn.execute(get_data_column_stmt).all():
                #             dw_column_dict.append(dict_row[0])

                #     if dw_column_dict != []:
                #         df = pd.read_csv(full_file_name, sep=link_file_sprtr)
                #         df.columns = dw_column_dict
                #         df.to_csv(full_file_name, index= False, sep=link_file_sprtr, encoding='utf-8-sig')

                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                    if row_count != 0:
                        logging.info(f"현재까지 파일 내 행 개수: {row_count}")

                    file_size = os.path.getsize(full_file_name)
                    logging.info(f"create_csv_file full_file_name::: {full_file_name}, file_size::: {file_size}")

                # tn_clct_file_info 수집파일정보
                tn_clct_file_info = CommonUtil.set_file_info(tn_clct_file_info, th_data_clct_mastr_log, tn_clct_file_info.insd_file_nm, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                
                if row_count == 0:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "n")
                    raise AirflowSkipException()
                else:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_DB, "y")
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_INT, "y")
            except AirflowSkipException as e:
                raise e
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_DB, "y")
                logging.info(f"create_csv_file Exception::: {e}")
                raise e
    
        file_path = create_directory(collect_data_list)
        db_connection_url_result = get_db_connection_url(collect_data_list)
        db_connection_url_result >> file_path >> create_csv_file(collect_data_list, db_connection_url_result, file_path)

    collect_data_list = select_collect_data_fail_info()
    db_to_csv_process.expand(collect_data_list = collect_data_list)

dag_object = rdb_to_csv_fail_retry()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2023,10,19,15,00,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
