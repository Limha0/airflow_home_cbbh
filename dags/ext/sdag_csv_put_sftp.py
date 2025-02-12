import logging
import urllib3

from datetime import datetime as dt
from pendulum import datetime, now
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

@dag(
    dag_id="sdag_csv_put_sftp",
    schedule="0 2 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["csv_put_sftp", "day", "ext"],
)
def sdag_5min_put_sftp():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    # SFTPHook 객체
    sftp_hook = SFTPHook(ssh_conn_id='ssh_inner_conn')

    @task
    def select_send_data_info(**kwargs):
        """
        th_data_clct_mastr_log 테이블에서 파일전송 대상 로그 정보 조회, tn_data_bsc_info 테이블에서 파일전송 대상 기본 정보 조회
        return: collect_data_list
        """
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul").add(days=-1).strftime("%Y%m%d") # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        # data_interval_start = now().strftime("%Y%m%d") # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        # 파일전송 대상 로그 정보 조회
        select_log_info_stmt = f'''
                            SELECT b.*
                            FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                            WHERE 1=1
                                AND a.dtst_cd = b.dtst_cd
                                AND LOWER(clct_yn) = 'y'
                                AND LOWER(link_yn) = 'y'
                                AND LOWER(a.dtst_cd) in ('data4', 'data799', 'data31', 'data855') -- 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회
                                AND COALESCE(stts_msg, '') != '{CONST.MSG_CLCT_COMP_NO_DATA}' -- 원천데이터 없음 제외
                                -- AND NOT (LOWER(b.step_se_cd) = '{CONST.STEP_CLCT}' AND LOWER(b.stts_cd) != '{CONST.STTS_COMP}') -- 마지막 수집단계 실패여도 전송
                                AND NOT (LOWER(b.step_se_cd) = '{CONST.STEP_DW_LDADNG}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') -- DW적재단계 성공 제외
                                AND b.clct_ymd != '{data_interval_start}'
                            ORDER BY b.clct_log_sn
                            '''
        logging.info(f"select_log_info_stmt !!!!!::: {select_log_info_stmt}")
        try:
            collect_data_list = CommonUtil.set_fail_info(session, select_log_info_stmt, kwargs)
        except Exception as e:    
            logging.info(f"select_send_data_info Exception::: {e}")
            raise e
        if collect_data_list == []:
            # logging.info("!!!!!!!!!!!",select_log_info_stmt)
            logging.info(f"select_send_data_info ::: 파일전송 대상없음 프로그램 종료")
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
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            file_path, full_file_path = CommonUtil.set_file_path(root_collect_file_path, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), tn_data_bsc_info)
            return file_path

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
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            
            full_file_name = root_collect_file_path + file_path + tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn
            copy_file_name = root_collect_file_path + file_path + tn_clct_file_info.insd_file_nm + "_sample." + tn_clct_file_info.insd_file_extn  # 미리보기 파일

            # 미리보기 파일 생성
            with open(full_file_name, 'r') as input_file:
                lines = input_file.readlines()[:11]
            with open(copy_file_name, 'w') as output_file:
                output_file.writelines(lines)
                logging.info(f"미리보기 파일 생성::: {copy_file_name}")
                
            try:
                with session.begin() as conn:
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, collect_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_WORK, CONST.MSG_FILE_INSD_SEND_WORK, "n")

                    pvdr_site_nm = tn_data_bsc_info.pvdr_site_nm
                    link_file_extn = tn_data_bsc_info.link_file_extn
                    pvdr_sou_data_pvsn_stle = tn_data_bsc_info.pvdr_sou_data_pvsn_stle
                    encrypt_key = kwargs['var']['value'].encrypt_key
                    full_file_path = root_collect_file_path + file_path

                    FileUtil.zip_file(full_file_path, pvdr_site_nm, link_file_extn, pvdr_sou_data_pvsn_stle)
                    encrypt_file = FileUtil.encrypt_file(full_file_path, pvdr_site_nm, encrypt_key, pvdr_sou_data_pvsn_stle)
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_FILE, "n")
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
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_EXT, "n")
                except Exception as e:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_TRANS_EXT, "n")
                    logging.info(f"put_file_sftp Exception::: {e}")
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
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_EXT, "n")
                    logging.info(f"check_loading_result dw_rcrd_cnt::: {result_count}")
            except Exception as e:
                logging.error(f"check_loading_result Exception::: {e}")
                raise e

        file_path = create_directory(collect_data_list)
        encrypt_file_path = encrypt_zip_file(collect_data_list, file_path)
        file_path >> encrypt_file_path >> put_file_sftp(collect_data_list, encrypt_file_path) >> check_loading_result(collect_data_list)
    
    collect_data_list = select_send_data_info()
    call_url_process.expand(collect_data_list = collect_data_list)

dag_object = sdag_5min_put_sftp()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024,2,4,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )