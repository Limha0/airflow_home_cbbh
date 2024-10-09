import logging
import urllib3
import json

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
from airflow.exceptions import AirflowSkipException, AirflowException
from sqlalchemy.orm import Session
from airflow.utils.db import provide_session
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dag(
    dag_id="sdag_sftp_bis_day",
    schedule="40 0 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["sftp_bis", "day", "ext"],
)
def sdag_sftp_bis():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    # SFTPHook 객체
    sftp_hook = SFTPHook(ssh_conn_id='ssh_inner_conn')
    bis_sftp_hook = SFTPHook(ssh_conn_id='ssh_bis_conn')

    @provide_session
    def check_connection_password(conn_id, session: Session = None):
        from airflow.models.connection import Connection
        """
        connection 비밀번호 확인
        params: conn_id: str, session
        """
        # conn_id 연결 정보 조회
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        new_password = current_password = conn.get_password()
        
        if conn:
            password_list = json.loads(conn.get_extra())['password_list']  # extra에 저장된 password_list
            if current_password in password_list:
                init_index = index = password_list.index(current_password)  # 초기 인덱스
            else:
                index = 0

            while True:
                status = bis_sftp_hook.test_connection()[0]
                if status:
                    if new_password != current_password:
                        # 변경된 비밀번호 커밋
                        conn.set_password(new_password)
                        session.commit()
                        logging.info(f"check_connection_password ::: '{conn_id}' Connection 비밀번호 업데이트")
                    break

                # 연결 실패 시 다음 비밀번호로 인덱스 이동
                index = (index + 1) % len(password_list)
                if index == init_index:  # 초기 인덱스로 회귀 시 종료
                    logging.info(f"check_connection_password ::: '{conn_id}' Connection에 일치 비밀번호 없음")
                    raise AirflowException()
                
                new_password = password_list[index]
                bis_sftp_hook.password = new_password
        else:
            logging.error(f"check_connection_password ERROR ::: '{conn_id}'에 해당하는 연결 정보 없음")
            raise AirflowException()
        
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
                                AND LOWER(link_clct_mthd_dtl_cd) = 'sftp'
                                AND LOWER(link_clct_cycle_cd) = 'day'
                                AND link_ntwk_otsd_insd_se = '외부'
                            ORDER BY sn
                            '''
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
        collect_data_list = CommonUtil.insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs)
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return collect_data_list

    @task
    def check_sftp_connection(collect_data_list, **kwargs):
        """
        sftp connection 연결 확인
        params: collect_data_list
        """
        try:
            check_connection_password('ssh_bis_conn')
            for collect_data_dict in collect_data_list:
                th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_dict['th_data_clct_mastr_log'])
                log_full_file_path = collect_data_dict['log_full_file_path']
                tn_clct_file_info = TnClctFileInfo(**collect_data_dict['tn_clct_file_info'])
        except Exception as e:
            for collect_data_dict in collect_data_list:
                th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_dict['th_data_clct_mastr_log'])
                log_full_file_path = collect_data_dict['log_full_file_path']
                tn_clct_file_info = TnClctFileInfo(**collect_data_dict['tn_clct_file_info'])
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CNTN, CONST.STTS_ERROR, CONST.MSG_CNTN_ERROR_SFTP, "n")
            logging.info(f"check_sftp_connection Exception::: {e}")
            raise e

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
            data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, data_interval_end, root_collect_file_path, "n")
            return file_path

        def check_file_exist(file, bis_file_path, full_file_path):
            """
            파일 존재 확인 및 원천 파일 복사
            params: file, bis_file_path, full_file_path
            return: file_exist, copy_source_full_file_name
            """
            source_file_name = file  # 원천 파일명
            source_full_file_name = bis_file_path + source_file_name
            copy_source_full_file_name = full_file_path + file
            # shutil.copyfile(source_full_file_name, copy_source_full_file_name) # local test
            bis_sftp_hook.retrieve_file(source_full_file_name, copy_source_full_file_name)
            file_exist = True
            return file_exist, copy_source_full_file_name
        
        @task
        def call_url(collect_data_list, file_path, **kwargs):
            """
            경로 내 파일 존재 확인 및 csv 파일 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
            return: file_size
            """
            import os
            from util.call_url_util import NoDataException
            from util.file_util import FileUtil
            import pandas as pd
            from util.date_custom_util import DateUtil
            import shutil

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']
            data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
            bis_file_path = kwargs['var']['value'].bis_file_path  # 원천 파일 경로
            bis_file_path += data_crtr_pnttm[:4] + "/" + data_crtr_pnttm[4:6] + "/" + data_crtr_pnttm[6:8] + "/"
            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            pvdr_data_se_vl_one = tn_data_bsc_info.pvdr_data_se_vl_one
            if dtst_cd == "data891":  # 노선별OD데이터
                bis_file_path += pvdr_data_se_vl_one + "/"
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path

            header = True   # 파일 헤더 모드
            mode = "w"  # 파일 쓰기 모드 overwrite
            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
            source_file_name = None  # 원천 파일명
            copy_source_full_file_name = None  # 복사된 원천 파일
            full_file_path = root_collect_file_path + file_path
            full_file_name = full_file_path + file_name
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr
            file_size = 0  # 파일 사이즈
            row_count = 0  # 행 개수
            file_exist = False  # 파일 존재 여부

            try:
                # 경로 존재 확인
                # if not os.path.exists(bis_file_path):  # local test
                if not bis_sftp_hook.path_exists(bis_file_path):
                    raise NoDataException('원천경로 없음')
                # 경로 내 파일 존재 확인
                # for file in os.listdir(bis_file_path):  # local test
                file_list = bis_sftp_hook.list_directory(bis_file_path)
                for file in file_list:
                    if dtst_cd == "data891":  # 노선별OD데이터
                        full_file_path = root_collect_file_path + file_path + file.split('_')[0] + "/"
                        os.makedirs(full_file_path, exist_ok=True)
                        file_exist, copy_source_full_file_name = check_file_exist(file, bis_file_path, full_file_path)
                    else:
                        if pvdr_data_se_vl_one in file:
                            file_exist, copy_source_full_file_name = check_file_exist(file, bis_file_path, full_file_path)
                
                            encodings = ['utf-8', 'euc-kr', 'cp949']  # 가능한 인코딩 목록
                            for encoding in encodings:
                                try:
                                    df = pd.read_csv(copy_source_full_file_name, delimiter='|', encoding=encoding)
                                    df.columns = df.columns.str.replace('\t', '')  # 헤더 컬럼 내 들여쓰기 제거
                                    df = df.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis = 0)  # 개행문자 제거, string 양 끝 공백 제거
                                    df['data_crtr_pnttm'] = th_data_clct_mastr_log.data_crtr_pnttm
                                    df['clct_pnttm'] = DateUtil.get_ymdhm()
                                    df['clct_log_sn'] = th_data_clct_mastr_log.clct_log_sn
                                    df.index += 1
                                    df.to_csv(full_file_name, index_label= "clct_sn", sep=link_file_sprtr, encoding='utf-8-sig')
                                    break
                                except UnicodeDecodeError:
                                    # 디코딩 에러가 발생한 경우 다음 인코딩 시도
                                    pass

                if file_exist == False:  # 파일 존재하지않을 때
                    raise NoDataException('원천파일 없음')
                
                if link_file_crt_yn == "y":
                    row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                    if row_count != 0:
                        logging.info(f"현재까지 파일 내 행 개수: {row_count}")
                    
                    # 파일 사이즈 확인
                    if os.path.exists(full_file_name):
                        file_size = os.path.getsize(full_file_name)
                    logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")
                else:  # 노선별OD데이터
                    # 복사된 원천 파일 사이즈 확인
                    if os.path.exists(copy_source_full_file_name):
                        file_size = os.path.getsize(copy_source_full_file_name)
                        row_count = file_size
                    logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")

                if row_count == 0:
                    raise NoDataException('원천파일내 데이터 없음')
                else:
                    # tn_clct_file_info 수집파일정보
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, tn_clct_file_info.insd_file_nm, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                    
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_DB, "n")
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_INT, "n")
            except NoDataException as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, e.msg, "n")
                raise e
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_DB, "n")
                logging.error(f"call_url Exception::: {e}")
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
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_WORK, CONST.MSG_FILE_INSD_SEND_WORK, "n")

                    pvdr_site_nm = tn_data_bsc_info.pvdr_site_nm
                    link_file_extn = tn_data_bsc_info.link_file_extn
                    pvdr_sou_data_pvsn_stle = tn_data_bsc_info.pvdr_sou_data_pvsn_stle
                    encrypt_key = kwargs['var']['value'].encrypt_key
                    root_collect_file_path = kwargs['var']['value'].root_collect_file_path
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
    
        file_path = create_directory(collect_data_list)
        encrypt_file_path = encrypt_zip_file(collect_data_list, file_path)
        file_path >> call_url(collect_data_list, file_path) >> encrypt_file_path >> put_file_sftp(collect_data_list, encrypt_file_path)
    
    collect_data_list = insert_collect_data_info()
    collect_data_list >> check_sftp_connection(collect_data_list) >> call_url_process.expand(collect_data_list = collect_data_list)

dag_object = sdag_sftp_bis()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        # execution_date=datetime(2024,1,12,15,00),
        execution_date=datetime(2024,4,4,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )