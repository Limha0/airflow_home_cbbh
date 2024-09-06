import logging
import urllib3

from pendulum import datetime, now
from airflow.decorators import dag, task
from util.common_util import CommonUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.operators.sftp import SFTPHook
from sqlalchemy.orm import sessionmaker
from airflow.exceptions import AirflowSkipException
from util.file_util import FileUtil
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dag(
    dag_id="sdag_api_to_csv_localdata",
    schedule="@monthly",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["api_to_csv", "month", "ext"],
)
def api_to_csv_localdata():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    # SFTPHook 객체
    sftp_hook = SFTPHook(ssh_conn_id='ssh_inner_conn')

    @task
    def insert_collect_data_info(**kwargs):
        """
        tn_data_bsc_info 테이블에서 수집 대상 기본 정보 조회
        return: collect_data_list
        """
        # 수집 대상 기본 정보 조회
        select_bsc_info_stmt = '''
                                select *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm 
                                from tn_data_bsc_info 
                                where 1 = 1
                                AND LOWER(clct_yn) = 'y'
                                    AND LOWER(link_yn) = 'y'
                                    AND LOWER(link_clct_mthd_dtl_cd) = 'on_file'
                                    AND LOWER(link_clct_cycle_cd) = 'month'
                                    AND link_dtst_se_cd ='set'  -- 연계데이터셋구분코드
                                AND link_ntwk_otsd_insd_se = '외부'
                                AND LOWER(dtst_cd) = 'data648' -- 지방행정인허가
                                order by dtst_dtl_cd ;
                                '''
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul").set(day=1)  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
        collect_data_list = []
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_bsc_info_stmt).all():
                    tn_data_bsc_info = TnDataBscInfo(**dict_row)
                    dtst_cd = tn_data_bsc_info.dtst_cd
                    data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start)
                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + "_" + data_crtr_pnttm
                    print("hello")
                    # th_data_clct_mastr_log 테이블에 insert
                    th_data_clct_mastr_log = ThDataClctMastrLog()
                    th_data_clct_mastr_log.dtst_cd = dtst_cd
                    th_data_clct_mastr_log.clct_ymd = data_interval_end.strftime("%Y%m%d")
                    th_data_clct_mastr_log.clct_data_nm = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                    # th_data_clct_mastr_log.gg_ctgry_cd = tn_data_bsc_info.pbadms_fld_lclsf_cd
                    # th_data_clct_mastr_log.pbadms_fld_cd = tn_data_bsc_info.pbadms_fld_cd
                    th_data_clct_mastr_log.data_crtr_pnttm = data_crtr_pnttm
                    th_data_clct_mastr_log.reclect_flfmt_times = 0
                    th_data_clct_mastr_log.step_se_cd = CONST.STEP_CNTN
                    th_data_clct_mastr_log.stts_cd = CONST.STTS_WORK
                    th_data_clct_mastr_log.stts_dt = now(tz="UTC")
                    th_data_clct_mastr_log.stts_msg = CONST.MSG_CNTN_WORK
                    th_data_clct_mastr_log.crt_dt = now(tz="UTC")
                    th_data_clct_mastr_log.link_file_sprtr = tn_data_bsc_info.link_file_sprtr
                    conn.add(th_data_clct_mastr_log)
                    conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)

                    # th_data_clct_stts_hstry_log 테이블에 insert
                    CommonUtil.insert_history_log(conn, th_data_clct_mastr_log, "n")

                    # tn_clct_file_info 수집파일정보 set
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                    # 수집로그파일 경로 생성
                    log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, data_interval_end, kwargs)

                    collect_data_list.append({
                                            "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                                            , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                            , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                            , "log_full_file_path" : log_full_file_path
                                            })

            # 수집로그파일 생성
            for log_data_list in collect_data_list:
                CommonUtil.create_log_file(log_data_list['log_full_file_path'], log_data_list['tn_clct_file_info'], session)
        except Exception as e:
            logging.info(f"insert_collect_data_info Exception::: {e}")
            raise e
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 수집 대상없음 프로그램 종료")
            raise AirflowSkipException()
        return collect_data_list[0]
    
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
        
    
    @task
    def call_url(collect_data_list, file_path, **kwargs):
        """
        URL 호출 및 zip 파일 압축 해제, th_data_clct_mastr_log 테이블에 입력
        params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
        return: file_size
        """
        import requests
        import os
        import time
        import zipfile
        import shutil
        import csv
        import pandas as pd
        from util.date_custom_util import DateUtil

        tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
        th_data_clct_mastr_log_temp = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])

        base_url = return_url = tn_data_bsc_info.link_data_clct_url
        root_collect_file_path = kwargs['var']['value'].root_collect_file_path
        full_file_path = root_collect_file_path + file_path

        retry_num = 0  # 재시도 횟수
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
        data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start)

        # 수집로그파일 생성
        log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, data_interval_end, kwargs)

        log_data_lists = []
        try:
            while True:
                # 재시도 5회 이상 시
                if retry_num >= 5:
                    break

                # url 설정
                return_url = f"{base_url}"
                
                # url 호출
                response = requests.get(return_url, verify= False, stream=True)                    
                response_code = response.status_code

                # url 호출 시 메세지 설정
                if retry_num == 0 :  # 첫 호출
                    logging.info(f"호출 url: {return_url}")
                elif retry_num != 0 :  # 재호출
                    logging.info(f"호출 결과 없음, url: {return_url} 로 재호출, 재시도 횟수: {retry_num}")

                if response_code == 200:
                    # zip 파일 다운
                    zip_file_full_name = full_file_path + return_url.split('/')[-1]
                    with open(zip_file_full_name, 'wb') as f:
                        for data in response.iter_content(chunk_size=4096):
                            f.write(data)
                    # zip 파일 압축 해제
                    with zipfile.ZipFile(zip_file_full_name, 'r') as zf:
                        os.chdir(full_file_path)
                        for zip_file in zf.infolist():
                            original_file_name = zip_file.filename.encode("cp437").decode("euc-kr").replace(" ", "_").replace("." + tn_data_bsc_info.link_file_extn ,"")  # 원천 파일 명
                            zip_file.filename = original_file_name + "_" + data_crtr_pnttm + "." + tn_data_bsc_info.link_file_extn  # 압축해제될 csv 파일
                            # zip_file.filename = original_file_name  # 압축해제될 csv 파일
                            full_file_name = full_file_path + zip_file.filename  # csv 파일 경로
                            zf.extract(zip_file)
                        # zip 파일 삭제
                        os.remove(zip_file_full_name)
                    break
                else:
                    logging.error(f"call_url response_code::: {response_code}")
                    retry_num += 1
                    time.sleep(5)
                    continue
            
            for file in os.listdir(full_file_path):
                if file.endswith(tn_data_bsc_info.link_file_extn) and not file.endswith('.zip'):
                # if not file.endswith('.zip'):
                    full_file_name = full_file_path + file
                    file_name = file.split('.csv')[0]
                    file_size = os.path.getsize(full_file_name)

                    # utf-8 (BOM) 로 변경
                    encodings = ['euc-kr', 'cp949', 'utf-8']  # 가능한 인코딩 목록
                    for encoding in encodings:
                        try:
                            temp_file_path = full_file_path + file_name + '_temp.csv'
                            with open(full_file_name, 'r', newline='', encoding=encoding) as input_file:
                                reader = csv.reader(input_file)
                                with open(temp_file_path, 'w', newline='', encoding='utf-8-sig') as output_file:
                                    writer = csv.writer(output_file)
                                    for row in reader:
                                        new_row = [field.replace('\n', '').replace('\r', '').replace('\r\n', '') for field in row]  # 개행 문자 제거
                                        writer.writerow(new_row[:-1])  # 각 행의 마지막 , 제거
                            shutil.move(temp_file_path, full_file_name)
                            break
                        except UnicodeDecodeError:
                            # 디코딩 에러가 발생한 경우 다음 인코딩 시도
                            pass
                            
                    # th_data_clct_mastr_log 테이블에 insert
                    th_data_clct_mastr_log = ThDataClctMastrLog()
                    th_data_clct_mastr_log.dtst_cd = tn_data_bsc_info.dtst_cd
                    th_data_clct_mastr_log.clct_ymd = data_interval_end.strftime("%Y%m%d")
                    th_data_clct_mastr_log.step_se_cd = CONST.STEP_CLCT
                    
                    # 행정분야 코드 조회
                    select_pbadms_fld_cd_stmt = f"""select pbadms_fld_lclsf_cd
                                                    from tn_data_bsc_info a RIGHT JOIN
                                                    (SELECT replace('{file_name}','_{data_crtr_pnttm}','') AS original_file_name) b
                                                    ON a.dtst_nm = b.original_file_name"""
                    with session.begin() as conn:
                        pbadms_fld_lclsf_cd = conn.execute(select_pbadms_fld_cd_stmt).first()[0]
                        # gg_ctgry_cd = conn.execute(select_pbadms_fld_cd_stmt).first()[1]
                        if pbadms_fld_lclsf_cd == None or pbadms_fld_lclsf_cd == '':  # 행정분야 코드, 파일명 불일치 시
                            th_data_clct_mastr_log.stts_cd = CONST.STTS_ERROR
                            th_data_clct_mastr_log.stts_msg = CONST.MSG_CLCT_ERROR_MATCH
                        elif file_size == 0:
                            th_data_clct_mastr_log.stts_cd = CONST.STTS_COMP
                            th_data_clct_mastr_log.stts_msg = CONST.MSG_CLCT_COMP_NO_DATA
                        else:
                            # th_data_clct_mastr_log.pbadms_fld_lclsf_cd = pbadms_fld_lclsf_cd
                            th_data_clct_mastr_log.stts_cd = CONST.STTS_COMP
                            th_data_clct_mastr_log.stts_msg = CONST.MSG_CLCT_COMP
                        th_data_clct_mastr_log.clct_data_nm = "지방행정인허가_" + file_name.split('_')[6]
                        # th_data_clct_mastr_log.gg_ctgry_cd = gg_ctgry_cd
                        th_data_clct_mastr_log.data_crtr_pnttm = data_crtr_pnttm
                        th_data_clct_mastr_log.reclect_flfmt_times = 0
                        th_data_clct_mastr_log.stts_dt = now(tz="UTC")
                        th_data_clct_mastr_log.crt_dt = now(tz="UTC")
                        th_data_clct_mastr_log.link_file_sprtr = tn_data_bsc_info.link_file_sprtr
                        conn.add(th_data_clct_mastr_log)
                        conn.commit()
                        conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                        
                        # csv 한글 헤더를 DW 영문 컬럼명으로 변경
                        get_data_column_stmt = f"""
                                    SELECT column_name
                                    FROM information_schema.columns
                                    WHERE table_name = (
                                        SELECT dw_tbl_phys_nm
                                        FROM tn_data_bsc_info
                                        WHERE LOWER(dtst_cd) = 'data648'
                                        AND dtst_nm = replace('{file_name}','_{data_crtr_pnttm}','')
                                        AND LOWER(dw_load_yn) = 'y'
                                    )
                                    AND column_name NOT IN ('data_crtr_pnttm','clct_sn','clct_pnttm','clct_log_sn','page_no')
                                    ORDER BY ordinal_position
                                """
                        with session.begin() as conn:
                            dw_column_dict = []  # DW 컬럼명
                            for dict_row in conn.execute(get_data_column_stmt).all():
                                dw_column_dict.append(dict_row[0])

                        if dw_column_dict != []:
                            df = pd.read_csv(full_file_name)
                            df = df.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis = 0)  # 개행문자 제거, string 양 끝 공백 제거
                            df.columns = dw_column_dict
                            df['data_crtr_pnttm'] = data_crtr_pnttm
                            df['clct_pnttm'] = DateUtil.get_ymdhm()
                            df['clct_log_sn'] = th_data_clct_mastr_log.clct_log_sn
                            df.index += 1
                            df.to_csv(full_file_name, index_label= "clct_sn", sep=tn_data_bsc_info.link_file_sprtr, encoding='utf-8-sig')

                        # th_data_clct_stts_hstry_log 테이블에 insert
                        CommonUtil.insert_history_log(conn, th_data_clct_mastr_log, "n")

                        if file_size != 0:
                            # tn_clct_file_info 수집파일정보
                            tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                            log_data_lists.append({
                                                "tn_data_bsc_info": tn_data_bsc_info.as_dict()
                                                , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                                , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                                })
                            CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
            conn.commit()
            
            # 수집로그파일 입력
            for log_data_list in log_data_lists:
                CommonUtil.create_log_file(log_full_file_path, log_data_list['tn_clct_file_info'], session)

            with session.begin() as conn:
                # 지방행정인허가 일회성 로그 제거
                delete_stmt = f"""
                    DELETE FROM th_data_clct_stts_hstry_log WHERE clct_log_sn = {th_data_clct_mastr_log_temp.clct_log_sn};
                    DELETE FROM th_data_clct_mastr_log WHERE clct_log_sn = {th_data_clct_mastr_log_temp.clct_log_sn};
                """
                conn.execute(delete_stmt)
        except AirflowSkipException as e:
            raise e
        except Exception as e:
            logging.error(f"call_url Exception::: {e}")
            # 일회성 로그 업데이트
            tn_clct_file_info_update = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info_update, session, th_data_clct_mastr_log_temp, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
            # 일회성 로그 제외한 성공 로그 존재 시 삭제
            delete_stmt = f"""
                DELETE FROM tn_clct_file_info WHERE LOWER(dtst_cd) = 'data648' and data_crtr_pnttm = '{data_crtr_pnttm}';
                DELETE FROM th_data_clct_stts_hstry_log WHERE clct_log_sn IN (
                    SELECT clct_log_sn FROM th_data_clct_mastr_log WHERE LOWER(dtst_cd) = 'data648' AND data_crtr_pnttm = '{data_crtr_pnttm}' AND clct_log_sn != {th_data_clct_mastr_log_temp.clct_log_sn}
                );
                DELETE FROM th_data_clct_mastr_log WHERE LOWER(dtst_cd) = 'data648' AND data_crtr_pnttm = '{data_crtr_pnttm}' AND clct_log_sn != {th_data_clct_mastr_log_temp.clct_log_sn};
            """
            with session.begin() as conn:
                conn.execute(delete_stmt)
            raise e
        return {
            "log_data_lists" : log_data_lists
            , "log_full_file_path" : log_full_file_path
            }
    
    @task
    def encrypt_zip_file(file_path, log_data_lists, **kwargs):
        """
        파일 압축 및 암호화
        params: file_path, log_data_lists
        return: encrypt_file
        """
        tn_data_bsc_info = TnDataBscInfo(**log_data_lists['log_data_lists'][0]['tn_data_bsc_info'])
        log_full_file_path = log_data_lists['log_full_file_path']
        try:
            with session.begin() as conn:
                for log_data_list in log_data_lists['log_data_lists']:
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, log_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    tn_clct_file_info = TnClctFileInfo(**log_data_list['tn_clct_file_info'])
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
            for log_data_list in log_data_lists['log_data_lists']:
                tn_clct_file_info = TnClctFileInfo(**log_data_list['tn_clct_file_info'])
                th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, log_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_FILE, "n")
                logging.error(f"encrypt_zip_file Exception::: {e}")
            raise e
        return {
                "file_path" : file_path,
                "encrypt_file" : encrypt_file
                }

    @task
    def put_file_sftp(encrypt_file_path, log_data_lists, **kwargs):
        """
        원격지 서버로 sftp 파일전송
        params: encrypt_file_path, log_data_lists
        """
        file_path = encrypt_file_path['file_path']
        encrypt_file = encrypt_file_path['encrypt_file']
        root_collect_file_path = kwargs['var']['value'].root_collect_file_path
        full_file_path = root_collect_file_path + file_path

        local_filepath = full_file_path + encrypt_file
        remote_filepath = kwargs['var']['value'].final_file_path + file_path
        
        log_full_file_path = log_data_lists['log_full_file_path']

        with session.begin() as conn:
            try:
                if not sftp_hook.path_exists(remote_filepath):
                    sftp_hook.create_directory(remote_filepath)
                sftp_hook.store_file(remote_filepath + encrypt_file, local_filepath)
                for log_data_list in log_data_lists['log_data_lists']:
                    tn_clct_file_info = TnClctFileInfo(**log_data_list['tn_clct_file_info'])
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, log_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_EXT, "n")
            except Exception as e:
                for log_data_list in log_data_lists['log_data_lists']:
                    tn_clct_file_info = TnClctFileInfo(**log_data_list['tn_clct_file_info'])
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, log_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_ERROR, CONST.MSG_FILE_INSD_SEND_ERROR_TRANS_EXT, "n")
                logging.error(f"put_file_sftp Exception::: {e}")
                raise e
    
    collect_data_list = insert_collect_data_info()
    file_path = create_directory(collect_data_list)
    log_data_lists = call_url(collect_data_list, file_path)
    encrypt_file_path = encrypt_zip_file(file_path, log_data_lists)
    file_path >> log_data_lists >> encrypt_file_path >> put_file_sftp(encrypt_file_path, log_data_lists)

dag_object = api_to_csv_localdata()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024,2,1,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )