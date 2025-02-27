from airflow.decorators import dag, task
import logging
import os
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
from datetime import datetime as dt
from sqlalchemy.orm import sessionmaker
from util.common_util import CommonUtil
from util.file_util import FileUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from airflow.providers.sftp.operators.sftp import SFTPHook

@dag(
    dag_id="sdag_csv_to_dw_hadoop",
    schedule="10,30,50 3,5,6 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["csv_to_dw", "day", "int"],
)
def csv_to_dw_hadoop():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')
    
    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    # SFTPHook 객체
    sftp_hook = SFTPHook(ssh_conn_id='ssh_db_conn')
    
    @task
    def select_ext_data_list_info(**kwargs):
        """
        (외부 수집 데이터) 복호화 대상 조회
        return: ext_data_list
        """
        ext_data_list = []
        try:
            with session.begin() as conn:
                # 외부 수집 복호화 대상 조회
                select_ext_stmt = get_select_stmt("union", "distinct", None, None, "ext", "unzip")
                for dict_row in conn.execute(select_ext_stmt).all():
                    th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                    # 기본 정보 조회
                    tn_data_bsc_info = get_bsc_info(th_data_clct_mastr_log.dtst_cd, th_data_clct_mastr_log.clct_data_nm, th_data_clct_mastr_log.clct_log_sn, conn)
                    pvdr_sou_data_pvsn_stle = tn_data_bsc_info.pvdr_sou_data_pvsn_stle

                    # 수집로그파일 경로
                    log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), kwargs)
                    
                    dtst_cd = tn_data_bsc_info.dtst_cd
                    if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                        (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                        # 파일 정보 조회
                        tn_clct_file_info = get_file_info(th_data_clct_mastr_log.clct_log_sn, conn)
                        file_path = tn_clct_file_info.insd_flpth

                        # 로그 업데이트
                        select_log_info_stmt_update = get_select_stmt(None, "update", file_path, pvdr_sou_data_pvsn_stle, "ext", "unzip")
                        for dict_row in conn.execute(select_log_info_stmt_update).all():
                            th_data_clct_mastr_log_update = ThDataClctMastrLog(**dict_row)
                            tn_clct_file_info_update = get_file_info(th_data_clct_mastr_log_update.clct_log_sn, conn)
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_update, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_WORK, CONST.MSG_FILE_STRGE_SEND_WORK, "n")
                    else:  # 노선별OD데이터 예외
                        file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                        # tn_clct_file_info 수집파일정보 set
                        tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)
                
                        # 로그 업데이트
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_WORK, CONST.MSG_FILE_STRGE_SEND_WORK, "n")

                    ext_data_list.append({
                                        "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                                        , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                        , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                        , "log_full_file_path": log_full_file_path
                                        })
        except Exception as e:
            logging.error(f"select_ext_data_list_info Exception::: {e}")
            raise e
        if ext_data_list == []:
            logging.info(f"select_ext_data_list_info ::: 복호화 대상없음")
        return ext_data_list

    @task
    def unzip_decrypt_file(ext_data_list, **kwargs):
        """
        (외부 수집 데이터) 파일 복호화 및 압축해제
        params: ext_data_list
        """
        for ext_data_dict in ext_data_list:
            tn_data_bsc_info = TnDataBscInfo(**ext_data_dict['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**ext_data_dict['tn_clct_file_info'])
            log_full_file_path = ext_data_dict['log_full_file_path']
            #final_file_path = kwargs['var']['value'].root_final_file_path  # local test
            final_file_path = kwargs['var']['value'].final_file_path
            
            dtst_cd = tn_data_bsc_info.dtst_cd
            pvdr_site_nm = tn_data_bsc_info.pvdr_site_nm
            encrypt_key = kwargs['var']['value'].encrypt_key
            pvdr_sou_data_pvsn_stle = tn_data_bsc_info.pvdr_sou_data_pvsn_stle
            
            if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                file_path = tn_clct_file_info.insd_flpth
            elif dtst_cd == 'data891':  # 노선별OD데이터 예외
                file_path = pvdr_site_nm + log_full_file_path[-9:]
            print("file_path!!!!!",file_path)
            full_file_path = final_file_path + file_path
            print("full_file_path!!!!!",full_file_path)
            try:
                FileUtil.decrypt_file(full_file_path, pvdr_site_nm, encrypt_key, pvdr_sou_data_pvsn_stle)
                FileUtil.unzip_file(full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle)
                
                if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                        (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                    # 로그 업데이트
                    select_log_info_stmt = get_select_stmt(None, "update", file_path, pvdr_sou_data_pvsn_stle, "ext", "unzip")
                    with session.begin() as conn:
                        for dict_row in conn.execute(select_log_info_stmt).all():
                            th_data_clct_mastr_log_update = ThDataClctMastrLog(**dict_row)
                            tn_clct_file_info_update = get_file_info(th_data_clct_mastr_log_update.clct_log_sn, conn)
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_update, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_WORK, CONST.MSG_FILE_STRGE_SEND_WORK_UNZIP, "n") # 압축해제 정상 작동 뒤에, 복호화 대상 조회 시작
                else:
                    # 로그 업데이트
                    th_data_clct_mastr_log = ThDataClctMastrLog(**ext_data_dict['th_data_clct_mastr_log'])
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_WORK, CONST.MSG_FILE_STRGE_SEND_WORK_UNZIP, "n")
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_WORK, CONST.MSG_FILE_STRGE_SEND_WORK_CHECK, "n")  # 최종경로 확인 건너뛰고 스토리지 전송하기 위해 로그 업데이트
            except Exception as e:
                if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                        (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                    # 로그 업데이트
                    select_log_info_stmt = get_select_stmt(None, "update", file_path, pvdr_sou_data_pvsn_stle, "ext", "unzip")
                    with session.begin() as conn:
                        for dict_row in conn.execute(select_log_info_stmt).all():
                            th_data_clct_mastr_log_update = ThDataClctMastrLog(**dict_row)
                            tn_clct_file_info_update = get_file_info(th_data_clct_mastr_log_update.clct_log_sn, conn)
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_update, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_STRGE_SEND_ERROR_UNZIP, "n")
                else:
                    # 로그 업데이트
                    th_data_clct_mastr_log = ThDataClctMastrLog(**ext_data_dict['th_data_clct_mastr_log'])
                    # tn_clct_file_info 수집파일정보 set
                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_STRGE_SEND_ERROR_UNZIP, "n")
                logging.error(f"unzip_decrypt_file Exception::: {e}")

    @task
    def check_file_in_final_path(**kwargs):
        """
        최종경로 파일 존재 여부 확인
        """
        select_log_info_stmt = get_select_stmt(None, None, None, None, None, "final")
        #final_file_path = kwargs['var']['value'].root_final_file_path  # local test
        final_file_path = kwargs['var']['value'].final_file_path

        dict_row = None
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_log_info_stmt).all():
                    th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                    tn_data_bsc_info = get_bsc_info(th_data_clct_mastr_log.dtst_cd, th_data_clct_mastr_log.clct_data_nm, th_data_clct_mastr_log.clct_log_sn, conn)
                    dtst_cd = tn_data_bsc_info.dtst_cd
                    # 수집로그파일 경로
                    log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), kwargs)

                    if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                        (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                        tn_clct_file_info = get_file_info(th_data_clct_mastr_log.clct_log_sn, conn)

                        file_path = tn_clct_file_info.insd_flpth
                        full_file_name = final_file_path + file_path + tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn
                        copy_file_name = final_file_path + file_path + tn_clct_file_info.insd_file_nm + "_sample." + tn_clct_file_info.insd_file_extn  # 미리보기 파일
                        result = FileUtil.check_file_exist(full_file_name)
                        
                        if result == True:  # 최종경로에 파일 존재할 때
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_WORK, CONST.MSG_FILE_STRGE_SEND_WORK_CHECK, "n")
                            
                            if dtst_cd not in {'data917','data4', 'data799', 'data31', 'data855'}:  # 경기도BMS시스템_SHP 제외
                                # 미리보기 파일 생성
                                with open(full_file_name, 'r') as input_file:
                                    lines = input_file.readlines()[:11]
                                with open(copy_file_name, 'w') as output_file:
                                    output_file.writelines(lines)
                                    logging.info(f"미리보기 파일 생성::: {copy_file_name}")
                        else:
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_STRGE_SEND_ERROR_CHECK, "n")
                            logging.error(f"check_file_in_final_path check_file_exist False::: {full_file_name}")
                    else:
                        file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                        # tn_clct_file_info 수집파일정보 set
                        tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)
                        full_file_name = final_file_path + file_name + "." + tn_clct_file_info.insd_file_extn
                        result = FileUtil.check_file_exist(full_file_name)

                        if result == True:  # 최종경로에 파일 존재할 때
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_COMP, CONST.MSG_FILE_STRGE_SEND_WORK_CHECK, "n")
                        else:
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_STRGE_SEND_ERROR_CHECK, "n")
                            logging.error(f"check_file_in_final_path check_file_exist False::: {full_file_name}")
        except Exception as e:
            logging.error(f"check_file_in_final_path Exception::: {e}")
        if dict_row == None:
            logging.info(f"check_file_in_final_path ::: 최종경로 파일 존재 여부 확인 대상없음")

    @task
    def get_active_namenode(**kwargs):
        import subprocess
        import base64
        import logging
        from airflow.models import Variable
        from airflow.providers.ssh.operators.ssh import SSHOperator
        from airflow.exceptions import AirflowException
        from airflow.providers.ssh.hooks.ssh import SSHHook
        """
        Active 네임노드를 확인하는 함수
        """
     
        command = "source /etc/profile && hdfs haadmin -getAllServiceState"
    
        # SSHOperator 설정
        ssh_operator = SSHOperator(
            task_id='get_active_namenode',
            ssh_conn_id='ssh_hadoop_conn',  # SSH 연결 ID (Airflow에서 설정)
            command=command,
            do_xcom_push=True  # XCom에 결과 저장
        )
        
        # SSHOperator 실행 및 결과 처리
        try:
            # result = ssh_operator.execute(context=kwargs)  # 명령 실행
            # logging.info(f"명령어 결과: {result}")

            # # Base64 디코딩
            # decoded_result = base64.b64decode(result).decode('utf-8')
            # logging.info(f"디코딩된 명령어 결과: {decoded_result}")
            
            # # 줄바꿈을 기준으로 결과 분리 및 공백 제거
            # lines = [line.strip() for line in decoded_result.split('\n') if line.strip()]
            ssh_hook = SSHHook(ssh_conn_id='ssh_hadoop_conn')
            ssh_client = ssh_hook.get_conn()

            # 명령 실행
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode('utf-8')
            logging.info(f"명령어 결과: {result}")

            # 줄바꿈을 기준으로 결과 분리 및 공백 제거
            lines = [line.strip() for line in result.split('\n') if line.strip()]

            # Active 네임노드 파싱
            active_namenode = None
            for line in lines:
                if "namenode01.gbgs.go.kr:8020" in line and "active" in line:
                    #active_namenode = "192.168.1.23"  #local test
                    active_namenode = "172.25.20.91"
                elif "namenode02.gbgs.go.kr:8020" in line and "active" in line:
                    #active_namenode = "192.168.1.24"   #local test
                    active_namenode = "172.25.20.92" 
            
            logging.info(f"Active 네임노드: {active_namenode}")
            return active_namenode
        
        except Exception as e:
            logging.error(f"명령어 실패: {e}")
            raise AirflowException(f"SSH operator error: {e}")

    @task
    def move_file_to_hadoop(active_namenode, **kwargs):
        from hdfs import InsecureClient
        from hdfs import Client
        import os
        import logging
        from airflow.exceptions import AirflowException
        import urllib.parse

        """
        HDFS 클라이언트를 사용하여 Hadoop 클러스터로 파일 전송
        """

        dict_row = None

        with session.begin() as conn:
            # 스토리지 전송 대상 조회
            select_log_info_stmt = get_select_stmt(None, "distinct", None, None, None, "send")
            for dict_row in conn.execute(select_log_info_stmt).all():
                th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                tn_data_bsc_info = get_bsc_info(th_data_clct_mastr_log.dtst_cd, th_data_clct_mastr_log.clct_data_nm, th_data_clct_mastr_log.clct_log_sn, conn)
                dtst_cd = tn_data_bsc_info.dtst_cd

                # 수집로그파일 경로
                log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd, "%Y%m%d"), kwargs)

                # 경기도 BMS시스템 또는 KOSIS의 데이터 처리 조건
                if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or \
                    (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):
                    tn_clct_file_info = get_file_info(th_data_clct_mastr_log.clct_log_sn, conn)
                    file_path = tn_clct_file_info.insd_flpth
                else:
                    file_path = tn_data_bsc_info.pvdr_site_nm + log_full_file_path[-9:]

                #before_file_path = kwargs['var'][value'].root_final_file_path + file_path  # local test 파일 경로
                before_file_path = kwargs['var']['value'].final_file_path + file_path
                hadoop_full_path = kwargs['var']['value'].hadoop_base_path + file_path  # 최종 Hadoop 경로
                link_file_extn = tn_data_bsc_info.link_file_extn #확장자
                pvdr_sou_data_pvsn_stle = tn_data_bsc_info.pvdr_sou_data_pvsn_stle # 제공형태
                
                print("@@@@@@@before_file_path : "+ before_file_path)
                file_name = tn_clct_file_info.insd_file_nm + '.' + tn_clct_file_info.insd_file_extn  # 파일 이름

                
                print("@@@@@@@hadoop_full_path : "+ hadoop_full_path)

                # HDFS 클라이언트 설정
                try:
                    if not active_namenode:
                        raise AirflowException("Active 네임노드를 찾을 수 없습니다.")

                    # client = InsecureClient(f"{active_namenode}", user='gsdpmng', root=hadoop_full_path)
                    client = InsecureClient(f"http://{active_namenode}:9870", user='gsdpmng', root=hadoop_full_path)
                    # client = Client(active_namenode, root=hadoop_full_path)

                    # HDFS에 디렉토리 생성 (존재하지 않는 경우)
                    client.makedirs(hadoop_full_path)
                    # URL 인코딩된 경로를 디코딩하여 로그 출력
                    # encoded_path = f"{active_namenode}{hadoop_full_path}"
                    # decoded_path = urllib.parse.unquote(encoded_path)
                    # logging.info(f"Active 네임노드에 경로 생성됨: {decoded_path}")


                    logging.info(f"로컬 파일 경로: {before_file_path}")
                    logging.info(f"HDFS 기본 경로: {hadoop_full_path}")
                    # logging.info(f"Active 네임노드에 경로 생성됨: {active_namenode}{hadoop_full_path}")

                    # 로컬에서 HDFS로 파일 전송
                    for file in os.listdir(before_file_path):
                        print("#######before_file_path : "+ before_file_path)
                        local_file_path = os.path.join(before_file_path, file)
                        print("#######local_file_path : "+ local_file_path)
                        #if file == file_name and os.path.isfile(local_file_path):
                        # file_name과 일치하는 모든 CSV 파일을 찾음
                        #if file.endswith('.csv') and os.path.isfile(local_file_path):
                        # '_sample.csv'이 포함되지 않고, 확장자가 .csv인 모든 파일 전송
                        if file.endswith('.csv') and '_sample.csv' not in file and os.path.isfile(local_file_path):
                            print("#######file : "+ file)
                          #  print("#######file_name : "+ file_name)
                            hdfs_file_path = f"{hadoop_full_path}{file}"
                            print("#######hdfs_file_path : "+ hdfs_file_path)

                            # HDFS에 파일 업로드
                            with open(local_file_path, 'rb') as f:
                                client.write(hdfs_file_path, f, overwrite=True)
                                #print("test : " ,client.write(hdfs_file_path, f, overwrite=True))
                            logging.info(f"파일이 Active 네임노드로 전송됨: {hdfs_file_path}")

                    if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                        (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                        # 로그 업데이트
                        select_log_info_stmt_update = get_select_stmt(None, "update", file_path, pvdr_sou_data_pvsn_stle, None, "send")
                        for dict_row in conn.execute(select_log_info_stmt_update).all():
                            th_data_clct_mastr_log_update = ThDataClctMastrLog(**dict_row)
                            tn_clct_file_info_update = get_file_info(th_data_clct_mastr_log_update.clct_log_sn, conn)
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_update, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_COMP, CONST.MSG_FILE_STRGE_SEND_COMP, "n")
                            if th_data_clct_mastr_log_update.dtst_cd in {'data4', 'data799', 'data31', 'data855'}:
                                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_update, CONST.STEP_DW_LDADNG, CONST.STTS_COMP, CONST.MSG_DW_LDADNG_COMP, "n")
                    else:
                        file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                        # tn_clct_file_info 수집파일정보 set
                        tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                        # 로그 업데이트
                        th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_COMP, CONST.MSG_FILE_STRGE_SEND_COMP, "n")
                except Exception as e:
                    if tn_data_bsc_info.link_file_crt_yn.lower() == 'y' or dtst_cd == 'data917' or\
                        (tn_data_bsc_info.link_clct_mthd_dtl_cd.lower() == 'on_file' and tn_data_bsc_info.pvdr_site_cd.lower() == 'ps00010'):  # 경기도BMS시스템_SHP, kosis_download
                        # 로그 업데이트
                        select_log_info_stmt_update = get_select_stmt(None, "update", file_path, pvdr_sou_data_pvsn_stle, None, "send")
                        for dict_row in conn.execute(select_log_info_stmt_update).all():
                            th_data_clct_mastr_log_update = ThDataClctMastrLog(**dict_row)
                            tn_clct_file_info_update = get_file_info(th_data_clct_mastr_log_update.clct_log_sn, conn)
                            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_update, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_STRGE_SEND_ERROR_MOVE, "n")
                    else:
                        file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                        # 로그 업데이트
                        th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                        # tn_clct_file_info 수집파일정보 set
                        tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_STRGE_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_STRGE_SEND_ERROR_MOVE, "n")
                    logging.error(f"move_file_to_hadoop Exception::: {e}")
        if dict_row == None:
            logging.info(f"move_file_to_hadoop ::: 하둡 전송 대상 조회 대상없음")

    def get_bsc_info(dtst_cd, clct_data_nm, clct_log_sn, conn):
        """
        기본 정보 조회
        params: dtst_cd, clct_data_nm, clct_log_sn
        return: dict_row_info
        """
        select_bsc_info_stmt = f'''
            SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
            FROM tn_data_bsc_info
            WHERE 1=1
            AND dtst_cd = '{dtst_cd}'
            ORDER BY dtst_cd
        '''
        # if dtst_cd == "data675":  # 신문고
        if dtst_cd.strip().lower() == "data675": 
            select_bsc_info_stmt = f'''
                SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                FROM tn_data_bsc_info 
                WHERE 1=1
                AND dtst_cd = 'data675'
                AND dtst_nm = '{clct_data_nm}'
                AND dtst_nm != '국민신문고_신문고민원'
                ORDER BY dtst_cd
            '''
        if dtst_cd == "data648":  # 지방행정인허가 (20240909)
            select_bsc_info_stmt = f'''
                 SELECT sn, dtst_cd, dtst_nm, dtst_expln, pvdr_site_cd, pvdr_inst_cd, pvdr_dept_nm, pvdr_pvsn_mthd_cd, pvdr_data_se_vl_one, pvdr_data_se_vl_two
                        , pvdr_updt_cycle_cd, pvdr_sou_data_pvsn_stle, link_ntwk_otsd_insd_se, link_pvdr_url, link_data_clct_url, link_db_id, link_tbl_phys_nm, link_dtst_se_cd, link_file_crt_yn
                        , link_file_merg_yn, link_file_extn, link_file_sprtr, link_yn, link_se_cd, clct_yn, link_clct_mthd, link_clct_mthd_dtl_cd, link_clct_cycle_cd, link_clct_job_id
                        , pbadms_fld_lclsf_cd, non_idntf_prcs_yn, encpt_yn, dw_load_yn, dw_load_mthd_cd, dw_tbl_phys_nm, addr_refine_yn, dtwrh_utlz_yn, data_rls_se_cd
                        , portal_dwnld_pvsn_yn, use_yn, crt_dt, rmrk, rmrk_two, rmrk_three, data_se_col_one, data_se_col_two, rfrnc_phys_tbl_nm, rfrnc_col_nm, crtr_del_col_nm
                        , (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                        FROM tn_data_bsc_info a
                        WHERE dtst_nm = (
                        SELECT LEFT(b.insd_file_nm,-7)
                        FROM th_data_clct_mastr_log a, tn_clct_file_info b
                        WHERE a.clct_log_sn = b.clct_log_sn
                        AND a.clct_log_sn = '{clct_log_sn}'
                    )
            '''
        dict_row_info = conn.execute(select_bsc_info_stmt).first()
        logging.info(f"select_file_info_stmt::: {select_bsc_info_stmt}")
        tn_data_bsc_info = TnDataBscInfo(**dict_row_info)
        return tn_data_bsc_info
    
    def get_file_info(clct_log_sn, conn):
        """
        파일 정보 조회
        params: clct_log_sn
        return: tn_clct_file_info
        """
        select_file_info_stmt = f'''
                                SELECT *
                                FROM tn_clct_file_info
                                WHERE 1=1
                                AND clct_log_sn = '{clct_log_sn}'
                                '''
        dict_row_file = conn.execute(select_file_info_stmt).first()
        tn_clct_file_info = TnClctFileInfo(**dict_row_file)
        return tn_clct_file_info

    def get_select_stmt(union_se, set_stmt, file_path, pvdr_sou_data_pvsn_stle, link_ntwk_extrl_inner_se, where_se):
        """
        select_stmt 설정
        params: union_se, set_stmt, file_path, pvdr_sou_data_pvsn_stle, link_ntwk_extrl_inner_se, where_se
        return: select_stmt
        """
        distinct_stmt = ""
        update_log_stmt = ""
        link_ntwk_extrl_inner_se_stmt = ""
        where_stmt = ""
        from_stmt = ""
        union_stmt = ""

        if set_stmt == "distinct":  # 수집 파일 확장자, 제공처 원천 데이터 제공_형태, 파일경로에 따른 중복 제거
            distinct_stmt = "DISTINCT ON (a.link_file_extn, a.pvdr_sou_data_pvsn_stle, c.insd_flpth)"
        if set_stmt == "update":  # 파일경로, 제공처 원천 데이터 제공 형태에 따른 업데이트 대상 로그 조회
            update_log_stmt = f"""
                                AND c.insd_flpth = '{file_path}'
                                AND a.pvdr_sou_data_pvsn_stle = '{pvdr_sou_data_pvsn_stle}'
                                """
        if link_ntwk_extrl_inner_se == "ext":  # 외부 수집 데이터
            link_ntwk_extrl_inner_se_stmt = "AND link_ntwk_otsd_insd_se = '외부'"
        
        if where_se == "unzip":  # (외부 수집 데이터) 복호화 대상 조회
            where_stmt = f"""AND ((LOWER(b.step_se_cd) = '{CONST.STEP_FILE_INSD_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') OR
                (b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_UNZIP}' OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_CHECK}' OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_WORK}'))
                AND b.clct_log_sn = c.clct_log_sn """
            from_stmt = f", tn_clct_file_info c"
        if union_se == "union":  # (외부 수집 데이터) 복호화 대상 조회 union 추가
            
            # 노선별OD데이터
            union_stmt += f'''UNION ALL
                SELECT DISTINCT ON (b.clct_ymd) b.*
                FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                WHERE 1=1
                AND a.dtst_cd = b.dtst_cd
                AND LOWER(a.use_yn) = 'y'
                AND LOWER(a.dtst_cd) IN ('data891')
                AND link_ntwk_otsd_insd_se = '외부'
                AND ((LOWER(b.step_se_cd) = '{CONST.STEP_FILE_INSD_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') OR
                (b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_UNZIP}' OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_CHECK}' OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_WORK}'))
            '''

        if where_se == "final":  # 최종경로 파일 존재 여부 확인 대상 조회
            where_stmt = f"""AND ((a.link_ntwk_otsd_insd_se = '외부' AND b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_WORK_UNZIP}') OR
                            (link_ntwk_otsd_insd_se = '내부' AND (LOWER(b.step_se_cd) = '{CONST.STEP_FILE_INSD_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}')) OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_CHECK}')"""
        if where_se == "send":  # 하둡 전송 대상 조회
            where_stmt = f"""AND (b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_WORK_CHECK}' OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_MOVE}')
                AND b.clct_log_sn = c.clct_log_sn """
            from_stmt = f", tn_clct_file_info c"

            if set_stmt != "update":
                # 노선별OD데이터
                union_stmt = f"""UNION ALL
                    SELECT DISTINCT ON (b.clct_ymd) b.*
                    FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                    WHERE 1=1
                    AND a.dtst_cd = b.dtst_cd
                    AND LOWER(a.use_yn) = 'y'
                    AND LOWER(a.dtst_cd) IN ('data891')
                    AND link_ntwk_otsd_insd_se = '외부'
                    AND (b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_WORK_CHECK}' OR b.stts_msg = '{CONST.MSG_FILE_STRGE_SEND_ERROR_MOVE}')
                """
        
        select_stmt = f'''
                            SELECT
                                {distinct_stmt} b.*
                            FROM tn_data_bsc_info a, th_data_clct_mastr_log b {from_stmt}
                            WHERE 1=1 
                                AND a.dtst_cd = b.dtst_cd
                                and a.dtst_dtl_cd  = b.dtst_dtl_cd 
                                AND LOWER(a.use_yn) = 'y'
                                AND LOWER(a.link_clct_mthd_dtl_cd) in ('on_file', 'open_api', 'esb', 'rdb', 'sftp','tcp')
                                AND LOWER(a.link_file_extn) IN ('csv', 'xls', 'zip')
                                AND LOWER(a.pvdr_inst_cd) != 'pi00011'
                                {where_stmt}
                                {link_ntwk_extrl_inner_se_stmt}
                                {update_log_stmt}
                                {union_stmt}
                        '''

        select_stmt = ' '.join(select_stmt.split())
        logging.info(f"select_stmt::: {select_stmt}")
        return select_stmt

    @task
    def select_loading_data_list_info(**kwargs):
        """
        th_data_clct_mastr_log 테이블에서 DW 적재 대상 조회
        return: loading_data_list
        """
        # 적재 대상 정보 조회
        select_log_info_stmt = f"""
                                SELECT
                                    b.*
                                FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                                WHERE 1=1 
                                    AND a.dtst_cd = b.dtst_cd
                                    AND LOWER(a.use_yn) = 'y'
                                    AND LOWER(a.dw_load_yn) = 'y'
                                    AND LOWER(a.link_clct_mthd_dtl_cd) in ('on_file', 'open_api', 'esb', 'rdb', 'sftp','tcp')
                                    AND LOWER(a.link_file_extn) IN ('csv', 'xls')
                                    AND LOWER(b.step_se_cd) NOT IN ('{CONST.STEP_CLCT}','{CONST.STEP_CNTN}','{CONST.STEP_FILE_INSD_SEND}')
                                    AND ((LOWER(b.step_se_cd) = '{CONST.STEP_FILE_STRGE_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') OR (LOWER(b.step_se_cd) = '{CONST.STEP_DW_LDADNG}' AND LOWER(b.stts_cd) != '{CONST.STTS_COMP}'))
                                    AND LOWER(a.dtst_cd) not in ('data4', 'data799', 'data31', 'data852', 'data855') -- 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 기상청_단기예보_시간, 실시간_측정정보_조회 제외
                                    AND LOWER(a.pvdr_inst_cd) != 'pi00011' -- 카페_게시글, 양주시_뉴스_기사, 네이버블로그 제외
                                    AND a.dtst_cd != 'data675' -- 신문고 제외
                                    AND LOWER(a.dtst_cd) != 'data648' -- 지방행정인허가 제외
                                UNION ALL
                                SELECT
                                    b.clct_log_sn, b.dtst_cd,  b.dtst_dtl_cd, b.clct_ymd, a.dtst_nm, b.data_crtr_pnttm, b.reclect_flfmt_nmtm, b.step_se_cd, b.stts_cd,
                                    b.stts_dt, b.stts_msg, b.crt_dt, b.dw_rcrd_cnt, b.creatr_id, b.creatr_nm, b.creatr_dept_nm, b.estn_field_one, b.estn_field_two, b.estn_field_three, b.link_file_sprtr
                                FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                                WHERE 1=1 
                                    AND a.dtst_cd = b.dtst_cd
                                    AND b.clct_data_nm = a.dtst_nm
                                    AND LOWER(a.use_yn) = 'y'
                                    AND LOWER(a.dw_load_yn) = 'y'
                                    AND LOWER(b.step_se_cd) NOT IN ('{CONST.STEP_CLCT}','{CONST.STEP_CNTN}','{CONST.STEP_FILE_INSD_SEND}')
                                    AND ((LOWER(b.step_se_cd) = '{CONST.STEP_FILE_STRGE_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') OR (LOWER(b.step_se_cd) = '{CONST.STEP_DW_LDADNG}' AND LOWER(b.stts_cd) != '{CONST.STTS_COMP}'))
                                    AND a.dtst_cd = 'data675' -- 신문고
                                UNION ALL
                                SELECT
                                    b.*
                                FROM tn_data_bsc_info a, th_data_clct_mastr_log b, tn_clct_file_info d
                                WHERE 1=1 
                                    AND a.dtst_cd = b.dtst_cd
                                    AND b.clct_log_sn = d.clct_log_sn
                                    AND a.dtst_nm = LEFT(d.insd_file_nm,-7)
                                    AND LOWER(a.use_yn) = 'y'
                                    AND LOWER(a.dw_load_yn) = 'y'
                                    AND LOWER(b.step_se_cd) NOT IN ('{CONST.STEP_CLCT}','{CONST.STEP_CNTN}','{CONST.STEP_FILE_INSD_SEND}')
                                    AND ((LOWER(b.step_se_cd) = '{CONST.STEP_FILE_STRGE_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') OR (LOWER(b.step_se_cd) = '{CONST.STEP_DW_LDADNG}' AND LOWER(b.stts_cd) != '{CONST.STTS_COMP}'))
                                    AND LOWER(a.dtst_cd) = 'data648' -- 지방행정인허가
                                ORDER BY clct_log_sn
                                """

        loading_data_lists = []
        with session.begin() as conn:
            for dict_row in conn.execute(select_log_info_stmt).all():
                th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                tn_data_bsc_info = get_bsc_info(th_data_clct_mastr_log.dtst_cd, th_data_clct_mastr_log.clct_data_nm, th_data_clct_mastr_log.clct_log_sn, conn)
                if tn_data_bsc_info.link_file_crt_yn.lower() == 'y':
                    tn_clct_file_info = get_file_info(th_data_clct_mastr_log.clct_log_sn, conn)
                else:
                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                    # tn_clct_file_info 수집파일정보 set
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                # 수집로그파일 경로
                log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), kwargs)
                loading_data_lists.append({
                                        "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                                        , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                        , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                        , "log_full_file_path": log_full_file_path
                                        })
        logging.info(f"select_stmt::: {select_log_info_stmt}")
        return loading_data_lists
    
    @task
    def create_temp_file_in_db(loading_data_lists, **kwargs):
        """
        DB서버에 임시폴더 생성 및 파일 전송
        params: loading_data_list
        """
        for loading_data_list in loading_data_lists:
            tn_data_bsc_info = TnDataBscInfo(**loading_data_list['tn_data_bsc_info'])
            th_data_clct_mastr_log = ThDataClctMastrLog(**loading_data_list['th_data_clct_mastr_log'])
            tn_clct_file_info = TnClctFileInfo(**loading_data_list['tn_clct_file_info'])
            log_full_file_path = loading_data_list['log_full_file_path']
            
            #final_file_path = kwargs['var']['value'].root_final_file_path  # local test
            final_file_path = kwargs['var']['value'].final_file_path
            db_ssh_temp_path = kwargs['var']['value'].db_ssh_temp_path
            print("final_file_path : " + final_file_path)
            print("db_ssh_temp_path : " + db_ssh_temp_path)


            file_name = ""
            full_file_name = ""
            if tn_data_bsc_info.link_file_crt_yn.lower() == 'y':
                file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn
                full_file_name = final_file_path + tn_clct_file_info.insd_flpth + file_name
            else:
                file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + ".csv"
                full_file_name = final_file_path + file_name
            
            with session.begin() as conn:
                try:
                    sftp_hook.create_directory(db_ssh_temp_path)
                    sftp_hook.store_file(db_ssh_temp_path + file_name, full_file_name)
                except Exception as e:
                    # 로그 업데이트
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                    CommonUtil.update_log_table(log_full_file_path, tn_data_bsc_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_ERROR, CONST.MSG_DW_LDADNG_ERROR_FILE, "n")
                    logging.error(f"create_temp_file_in_db Exception::: {e}")
                    raise e
    
    @task
    def get_data_type(loading_data_lists, **kwargs):
        """
        DW 컬럼데이터 타입획득 및 파일에서 컬럼명획득
        params: loading_data_list
        return: dw_column_dict, file_column
        """
        lists_column_info = []
        for loading_data_list in loading_data_lists:
            tn_data_bsc_info = TnDataBscInfo(**loading_data_list['tn_data_bsc_info'])
            th_data_clct_mastr_log = ThDataClctMastrLog(**loading_data_list['th_data_clct_mastr_log'])
            tn_clct_file_info = TnClctFileInfo(**loading_data_list['tn_clct_file_info'])
            log_full_file_path = loading_data_list['log_full_file_path']
            
            #final_file_path = kwargs['var']['value'].root_final_file_path  # local test
            final_file_path = kwargs['var']['value'].final_file_path
            if tn_data_bsc_info.link_file_crt_yn.lower() == 'y':
                file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn
                full_file_name = final_file_path + tn_clct_file_info.insd_flpth + file_name
            else:
                file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + ".csv"
                full_file_name = final_file_path + file_name 

            get_data_type_stmt = f"""
                SELECT ordinal_position, column_name, 
                    CASE WHEN udt_name = 'bpchar' AND character_maximum_length IS NOT NULL THEN udt_name || '(' || character_maximum_length || ')' 
                    ELSE udt_name 
                    END AS data_type
                FROM information_schema.columns
                WHERE table_name = '{tn_data_bsc_info.dw_tbl_phys_nm}'
                ORDER BY ordinal_position
            """

            with session.begin() as conn:
                try:
                    # DW 컬럼명, 데이터 타입
                    dw_column_dict = {}
                    for dict_row in conn.execute(get_data_type_stmt).all():
                        dw_column_dict[dict_row[1]] = dict_row[2]
                
                    # 파일 컬럼명
                    file_column = pd.read_csv(full_file_name, sep= th_data_clct_mastr_log.link_file_sprtr, low_memory = False).columns.str.lower()  # 소문자로 변경
                    lists_column_info.append({
                        "loading_data_list" : loading_data_list,
                        "dw_column_dict" : dw_column_dict,
                        "file_column" : file_column.tolist()
                    })
                except Exception as e:
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_ERROR, CONST.MSG_DW_LDADNG_ERROR_DATA, "n")
                    logging.error(f"get_data_type Exception::: {e}")
                    raise e
        return lists_column_info
    
    @task
    def dw_loading(lists_column_info, **kwargs):
        """
        DW 적재 및 DB 서버 임시파일삭제
        params: loading_data_list, column_info
        """
        for loading_data_list in lists_column_info:
            th_data_clct_mastr_log = ThDataClctMastrLog(**loading_data_list['loading_data_list']['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**loading_data_list['loading_data_list']['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**loading_data_list['loading_data_list']['tn_clct_file_info'])
            log_full_file_path = loading_data_list['loading_data_list']['log_full_file_path']
            db_ssh_temp_path = kwargs['var']['value'].db_ssh_temp_path
            print("dw_loading_file_name_db_ssh_temp_path :"+ db_ssh_temp_path)

            temp_table_name = "temp_" + tn_data_bsc_info.dw_tbl_phys_nm
            data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
            link_file_sprtr = th_data_clct_mastr_log.link_file_sprtr
            if tn_data_bsc_info.link_file_crt_yn.lower() == 'y':
                file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn
                print("YYYYYY_dw_loading_file_name : " + file_name)
            else:
                file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + ".csv"
                print("dw_loading_file_name : " + file_name)

            try:
                # DW 적재
                delete_temp_table(temp_table_name)
                create_temp_table(temp_table_name, loading_data_list)
                copy_temp_table(temp_table_name, loading_data_list, db_ssh_temp_path, file_name, link_file_sprtr)
                delete_table(tn_data_bsc_info, data_crtr_pnttm, temp_table_name)
                insert_table(tn_data_bsc_info, loading_data_list, temp_table_name)
                delete_temp_table(temp_table_name)

                # DW 적재 결과 확인 (copy 실패 확인)
                result_count = check_loading_result(tn_data_bsc_info, data_crtr_pnttm)
                if result_count == 0:  # count(data_crtr_pnttm)가 0일 때 실패로 판단
                    with session.begin() as conn:
                        th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_ERROR, CONST.MSG_DW_LDADNG_ERROR_DW, "n")
                        logging.error(f"dw_loading::: {CONST.MSG_DW_LDADNG_ERROR_DW}")
                else:
                    with session.begin() as conn:
                        th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                        th_data_clct_mastr_log.dw_rcrd_cnt = result_count
                        CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_COMP, CONST.MSG_DW_LDADNG_COMP, "n")
                        logging.info(f"dw_loading::: {CONST.MSG_DW_LDADNG_COMP}")

                # 임시파일 삭제
                if tn_data_bsc_info.dtst_cd not in {"data762", "data763"}:  # 부서정보, 직원정보 예외
                    sftp_hook.delete_file(db_ssh_temp_path + file_name)

            except Exception as e:
                with session.begin() as conn:
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_ERROR, CONST.MSG_DW_LDADNG_ERROR_DW, "n")
                logging.error(f"dw_loading::: {CONST.MSG_DW_LDADNG_ERROR_DW}")
                logging.error(f"dw_loading Exception::: {e}")
                raise e
    
    def dtype_mapping(file_column, column_dict):
        """
        데이터 타입 매핑
        params: file_column, column_dict
        return: dtypedict
        """
        dtypedict = {}
        for i,j in column_dict.items():
            for k in file_column:
                if i.lower() == k.lower():
                    dtypedict.update({k: j})
        return dtypedict

    def delete_temp_table(temp_table_name):
        """
        임시 테이블 삭제
        params: temp_table_name
        """
        try:
            drop_stmt = f"DROP TABLE IF EXISTS {temp_table_name}"
            logging.info(f"drop_stmt::: {drop_stmt}")
            with session.begin() as conn:
                conn.execute(drop_stmt)
        except Exception as e:
            logging.error(f"delete_temp_table Exception::: {e}")
    
    def create_temp_table(temp_table_name, loading_data_list):
        """
        임시 테이블 생성
        params: temp_table_name, loading_data_list
        """
        file_column = loading_data_list['file_column']
        dtypedict = dtype_mapping(file_column, loading_data_list['dw_column_dict'])
        
        logging.info(f"file_column::: {file_column}")
        logging.info(f"dw_column_dict::: {loading_data_list['dw_column_dict']}")
        try:
            create_stmt = f"""CREATE TABLE {temp_table_name} (
                {', '.join('"'+col+'" '+dtype for col, dtype in dtypedict.items())} )
            """
            logging.info(f"create_stmt::: {create_stmt}")
            with session.begin() as conn:
                conn.execute(create_stmt)
        except Exception as e:
            logging.error(f"create_temp_table Exception::: {e}")
    
    def delete_table(tn_data_bsc_info, data_crtr_pnttm, temp_table_name):
        """
        DW 테이블 delete
        params: tn_data_bsc_info, data_crtr_pnttm, temp_table_name
        """
        delete_stmt = ""
        crtr_del_col_nm = tn_data_bsc_info.crtr_del_col_nm  # 삭제 기준 컬럼
        
        dtst_cd = tn_data_bsc_info.dtst_cd.lower()
        pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
        dw_load_mthd_cd = tn_data_bsc_info.dw_load_mthd_cd.lower()
        dw_tbl_phys_nm = tn_data_bsc_info.dw_tbl_phys_nm
        pvdr_data_se_vl_one = tn_data_bsc_info.pvdr_data_se_vl_one

        if dw_load_mthd_cd == "append":
            delete_stmt = f"""
                DELETE FROM {dw_tbl_phys_nm}
                WHERE data_crtr_pnttm = '{data_crtr_pnttm}'
            """

            if crtr_del_col_nm != None and crtr_del_col_nm != '':
                delete_stmt = f"""
                    WITH crtr_list AS (
                        SELECT "{crtr_del_col_nm}" crtr_column
                        FROM {temp_table_name}
                        GROUP BY "{crtr_del_col_nm}"
                    )
                    DELETE FROM {dw_tbl_phys_nm}
                    WHERE "{crtr_del_col_nm}" IN ( SELECT crtr_column FROM crtr_list )
                """

                if dtst_cd == "data690":  # 조기집행내역
                    delete_stmt = f"""
                        WITH crtr_list AS (
                            SELECT {crtr_del_col_nm}
                            FROM {temp_table_name}
                            GROUP BY {crtr_del_col_nm}
                        )
                        DELETE FROM {dw_tbl_phys_nm}
                        WHERE anestmkcd IN ( SELECT anestmkcd FROM crtr_list )
                        AND acntdvcd IN ( SELECT acntdvcd FROM crtr_list )
                        AND dbizcd IN ( SELECT dbizcd FROM crtr_list )
                        AND erexymd IN ( SELECT anestmkcd FROM crtr_list )
                        AND fyr IN ( SELECT fyr FROM crtr_list )
                    """

                if dtst_cd == "data691":  # 집행실적세부사업별통계목별집계
                    delete_stmt = f"""
                        WITH crtr_list AS (
                            SELECT {crtr_del_col_nm}
                            FROM {temp_table_name}
                            GROUP BY {crtr_del_col_nm}
                        )
                        DELETE FROM {dw_tbl_phys_nm}
                        WHERE acntdvmstrcd IN ( SELECT acntdvmstrcd FROM crtr_list )
                        AND acntdvcd IN ( SELECT acntdvcd FROM crtr_list )
                        AND deptcd IN ( SELECT deptcd FROM crtr_list )
                        AND dbizcd IN ( SELECT dbizcd FROM crtr_list )
                        AND anestmkcd IN ( SELECT anestmkcd FROM crtr_list )
                        AND exeymd IN ( SELECT exeymd FROM crtr_list )
                        AND pbizcd IN ( SELECT pbizcd FROM crtr_list )
                        AND ubizcd IN ( SELECT ubizcd FROM crtr_list )
                        AND fyr IN ( SELECT fyr FROM crtr_list )
                    """
                    
                if dtst_cd == "data43":  # 창업온도
                    delete_stmt = f"""
                        WITH crtr_list AS (
                            SELECT {crtr_del_col_nm}
                            FROM {temp_table_name}
                            GROUP BY {crtr_del_col_nm}
                        )
                        DELETE FROM {dw_tbl_phys_nm}
                        WHERE crtryr IN ( SELECT crtryr FROM crtr_list )
                        AND crtrqt IN ( SELECT crtrqt FROM crtr_list )
                    """

            if pvdr_site_cd == "ps00010":  # 국가통계포털
                delete_stmt += f" AND tbl_id = '{pvdr_data_se_vl_one}'"

            if pvdr_site_cd == "ps00031":  # 생활SOC
                delete_stmt += f" AND are_cde = '{pvdr_data_se_vl_one}'"

        if dw_load_mthd_cd == "3month_change":
            delete_stmt = f"""
                DELETE FROM {dw_tbl_phys_nm}
                WHERE "{crtr_del_col_nm}"::timestamp between (to_date(LEFT('{data_crtr_pnttm}',6),'YYYYMM') - '3 month'::INTERVAL) and now()
            """
        
        if dw_load_mthd_cd == "change":
            delete_stmt = f"TRUNCATE TABLE {dw_tbl_phys_nm}"

        logging.info(f"delete_stmt::: {' '.join(delete_stmt.split())}")
        try:
            with session.begin() as conn:
                conn.execute(delete_stmt)
        except Exception as e:
            logging.error(f"delete_table Exception::: {e}")
        
    def copy_temp_table(temp_table_name, loading_data_list, db_ssh_temp_path, file_name, link_file_sprtr):
        """
        임시 테이블에 copy
        params: temp_table_name, loading_data_list, db_ssh_temp_path, tn_clct_file_info, link_file_sprtr
        """
        file_column = loading_data_list['file_column']
        
        #copy_stmt = f"""copy {temp_table_name} (
        #    "{'", "'.join(file_column)}"
        #) from '{db_ssh_temp_path}{file_name}' delimiter '{link_file_sprtr}' csv header encoding 'UTF-8';
        #"""

        copy_stmt = f"""copy {temp_table_name} (
            "{'", "'.join(file_column)}"
        ) from '/var/lib/postgresql/temp/DwTemp/{file_name}' delimiter '{link_file_sprtr}' csv header encoding 'UTF-8';
        """
        # local test

        logging.info(f"copy_stmt::: {' '.join(copy_stmt.split())}")
        try:
            with session.begin() as conn:
                conn.execute(copy_stmt)
        except Exception as e:
            logging.error(f"copy_temp_table Exception::: {e}")
    
    def insert_table(tn_data_bsc_info, loading_data_list, temp_table_name):
        """
        DW 테이블에 입력
        params: tn_data_bsc_info, loading_data_list, temp_table_name
        """

        dtst_cd = tn_data_bsc_info.dtst_cd.lower()
        dw_tbl_phys_nm = tn_data_bsc_info.dw_tbl_phys_nm
        file_column = loading_data_list['file_column']

        # if dtst_cd != 'data32':
        insert_stmt = f"""
            INSERT INTO {dw_tbl_phys_nm} (
                "{'", "'.join(file_column)}"
            ) SELECT "{'", "'.join(file_column)}" FROM {temp_table_name}
        """

        logging.info(f"insert_stmt::: {' '.join(insert_stmt.split())}")
        try:
            with session.begin() as conn:
                conn.execute(insert_stmt)
        except Exception as e:
            logging.error(f"insert_table Exception::: {e}")

    def check_loading_result(tn_data_bsc_info, data_crtr_pnttm):
        """
        DW 적재 결과 확인 (copy 실패 확인)
        params: tn_data_bsc_info, data_crtr_pnttm
        return: result
        """
        dw_tbl_phys_nm = tn_data_bsc_info.dw_tbl_phys_nm
        pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
        result_count = 0
        get_count_stmt = f"""SELECT COUNT(data_crtr_pnttm) FROM {dw_tbl_phys_nm} WHERE data_crtr_pnttm = '{data_crtr_pnttm}'"""
        if pvdr_site_cd == 'ps00010':
            pvdr_data_se_vl_one = tn_data_bsc_info.pvdr_data_se_vl_one
            get_count_stmt += f""" AND tbl_id = '{pvdr_data_se_vl_one}' """
        if pvdr_site_cd == 'ps00031':
            pvdr_data_se_vl_one = tn_data_bsc_info.pvdr_data_se_vl_one
            get_count_stmt += f""" AND are_cde = '{pvdr_data_se_vl_one}' """
        try:
            with session.begin() as conn:
                result_count = conn.execute(get_count_stmt).first()[0]
        except Exception as e:
            logging.error(f"check_loading_result Exception::: {e}")
        return int(result_count)

    
    ext_data_list = select_ext_data_list_info()
    active_namenode = get_active_namenode()
    loading_data_lists = select_loading_data_list_info()
    lists_column_info = get_data_type(loading_data_lists)

    ext_data_list >> unzip_decrypt_file(ext_data_list) >> check_file_in_final_path() >> move_file_to_hadoop(active_namenode) >> loading_data_lists >> create_temp_file_in_db(loading_data_lists) >> lists_column_info >> dw_loading(lists_column_info)

dag_object = csv_to_dw_hadoop()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024, 1, 29),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
