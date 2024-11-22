import logging
import pendulum

from pendulum import datetime, now
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
    dag_id="sdag_rdb_to_csv_int_day",
    schedule="0 1,5 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    # default_timezone=pendulum.timezone('Asia/Seoul'),  # 타임존 설정
    tags=["db_to_csv", "day", "int"],
)
def rdb_to_csv():

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
                                    AND LOWER(clct_yn) = 'y'
                                    AND LOWER(link_yn) = 'y'
                                    AND LOWER(link_clct_mthd_dtl_cd) = 'rdb'
                                    AND LOWER(link_clct_cycle_cd) = 'day'
                                    AND lower(pvdr_site_cd) ='ps00040' 
                                    AND link_ntwk_otsd_insd_se = '내부' --주민요약DB
                                ORDER BY sn
                                '''
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
        if now().strftime("%H") == '05':
            data_interval_start = now().add(days=-1)
        collect_data_list = CommonUtil.insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs)
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return collect_data_list

    @task_group(group_id='db_to_csv_process')
    def db_to_csv_process(collect_data_list):
        import os
        import pandas as pd
        
        @task
        def get_db_connection_url(collect_data_list, **kwargs):
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
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CNTN_ERROR, "n")
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
            data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            final_file_path = kwargs['var']['value'].final_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, data_interval_end, final_file_path, "n")
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

            dtst_cd = tn_data_bsc_info.dtst_cd
            
            select_db_stmt = f'''SELECT a.*
                                    ,'{data_crtr_pnttm}' data_crtr_pnttm
                                    ,'{DateUtil.get_ymdhm()}' clct_pnttm
                                    ,'{th_data_clct_mastr_log.clct_log_sn}' clct_log_sn
                                FROM {link_tbl_phys_nm} a '''

            if dtst_cd == 'data794':  # 주민요약DB_관내전입이력정보
                select_db_stmt += f" WHERE inport_ymd = '{data_crtr_pnttm}'"
            if dtst_cd == 'data795':  # 주민요약DB_관내전출이력정보
                select_db_stmt += f" WHERE export_ymd = '{data_crtr_pnttm}'"

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

                # 청크 단위로 데이터를 읽어오면서 처리
                # os.chdir(full_file_path)
                # file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
                # chunksize = 100000  # 원하는 청크 사이즈 설정
                # row_count = 0
                # file_size = 0
                # for chunk in pd.read_sql_query(select_db_stmt, db_engine, chunksize=chunksize):
                #     # 데이터 처리 및 개행 문자 제거
                #     chunk = chunk.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis=0)
                #     chunk.index += row_count + 1  # 인덱스 업데이트

                #     # CSV 파일에 청크 단위로 쓰기
                #     chunk.to_csv(file_name, sep=link_file_sprtr, mode='a', header=(row_count == 0), index_label="clct_sn", encoding='utf-8-sig')

                #     row_count += len(chunk)
                #     logging.info(f"현재까지 파일 내 행 개수: {row_count}")
                
                # full_file_name = full_file_path + file_name
                

                # --------------------------------------
                # # csv 파일 생성
                # os.chdir(full_file_path)
                # file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
                # df = pd.read_sql_query(select_db_stmt, db_engine)
                # df = df.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis = 0)  # 개행문자 제거, string 양 끝 공백 제거
                # df.index += 1
                # df.to_csv(file_name, sep = tn_data_bsc_info.link_file_sprtr, header = True, index_label= "clct_sn", mode='w', encoding='utf-8-sig')
                # full_file_name = full_file_path + file_name
                # --------------------------------------------

                # 쿼리 실행 후 수동으로 데이터 가져오기
                cursor = db_engine.cursor()
                cursor.execute(select_db_stmt)
                rows = cursor.fetchall()

                # 열 이름 가져오기
                columns = [desc[0] for desc in cursor.description]

                # Pandas DataFrame 생성
                df = pd.DataFrame(rows, columns=columns)

                # 개행문자 제거 및 string 양 끝 공백 제거
                df = df.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis = 0)
                df.index += 1

                # CSV 파일로 저장
                os.chdir(full_file_path)
                file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
                df.to_csv(file_name, sep=tn_data_bsc_info.link_file_sprtr, header=True, index_label="clct_sn", mode='w', encoding='utf-8-sig')
                full_file_name = full_file_path + file_name

                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                    if row_count != 0:
                        logging.info(f"현재까지 파일 내 행 개수: {row_count}")

                    file_size = os.path.getsize(full_file_name)
                    logging.info(f"create_csv_file full_file_name::: {full_file_name}, file_size::: {file_size}")

                # tn_clct_file_info 수집파일정보
                tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, tn_clct_file_info.insd_file_nm, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                
                if row_count == 0:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "n")
                    raise AirflowSkipException()
                else:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_DB, "n")
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_INT, "n")
            except AirflowSkipException as e:
                raise e
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_DB, "n")
                logging.info(f"create_csv_file Exception::: {e}")
                raise e
    
        file_path = create_directory(collect_data_list)
        db_connection_url_result = get_db_connection_url(collect_data_list)
        db_connection_url_result >> file_path >> create_csv_file(collect_data_list, db_connection_url_result, file_path)

    collect_data_list = insert_collect_data_info()
    db_to_csv_process.expand(collect_data_list = collect_data_list)

dag_object = rdb_to_csv()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024,1,30,15,00,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
