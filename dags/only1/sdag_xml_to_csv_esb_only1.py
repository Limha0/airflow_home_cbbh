import logging
import urllib3
import os
import re

from pendulum import datetime, now, from_format
from airflow.decorators import dag, task, task_group
from util.common_util import CommonUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.operators.sftp import SFTPHook
from sqlalchemy.orm import sessionmaker
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dag(
    dag_id="sdag_xml_to_csv_esb_only1",
    schedule="30 2 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["xml_to_csv_retry", "day", "int"],
)
def xml_to_csv_esb_fail_retry():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine)

    @task
    def insert_collect_data_info(**kwargs):
        """
        th_data_clct_mastr_log 테이블에서 재수집 대상 로그 정보 조회, tn_data_bsc_info 테이블에서 재수집 대상 기본 정보 조회
        return: collect_data_list
        """
        run_conf = ""
        if kwargs['dag_run'].conf != {}:
            dtst_cd = kwargs['dag_run'].conf['dtst_cd']
            run_conf = f"AND LOWER(a.dtst_cd) = '{dtst_cd}'"

        # 재수집 대상 로그 정보 조회
        select_bsc_info_stmt = f'''
                            SELECT sn, a.dtst_cd, b.dtst_nm, a.dtst_dtl_cd, a.pvdr_site_cd, pvdr_inst_cd, pvdr_dept_nm, pvdr_pvsn_mthd_cd, pvdr_data_se_vl_one, pvdr_data_se_vl_two,pvdr_data_se_vl_three
                                , pvdr_updt_cycle_cd, pvdr_sou_data_pvsn_stle, link_ntwk_otsd_insd_se, link_pvdr_url, link_data_clct_url, link_db_id, link_tbl_phys_nm, link_dtst_se_cd, link_file_crt_yn
                                , link_file_merg_yn, link_file_extn, a.link_file_sprtr, link_yn, link_se_cd,insd_trsm_yn, clct_yn, link_clct_mthd, link_clct_mthd_dtl_cd, link_clct_cycle_cd, link_clct_job_id,pbadms_fld_lclsf_cd
                                , encpt_yn, a.dw_load_yn, dw_load_mthd_cd, b.dw_tbl_phys_nm, addr_refine_yn, dtwrh_utlz_yn, data_rls_se_cd, geom_utlz_yn
                                , pbadms_fld_mclsf_cd, pbadms_fld_sclsf_cd, rel_kywd_nm, non_idntf_prcs_yn ,portal_dwnld_pvsn_yn, a.use_yn, crt_dt, rmrk, rmrk_two, rmrk_three, data_se_col_one, data_se_col_two, rfrnc_phys_tbl_nm, rfrnc_col_nm, crtr_del_col_nm
                                , (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND a.pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                            FROM tn_data_bsc_info a, tc_pbadms_fld_mapng b
                            WHERE 1=1
                                AND a.dtst_cd = b.dtst_cd
                                AND LOWER(clct_yn) = 'y'
                                AND LOWER(link_yn) = 'y'
                                AND LOWER(link_clct_mthd_dtl_cd) = 'esb'
                                AND LOWER(link_clct_cycle_cd) = 'day'
                                AND link_ntwk_otsd_insd_se = '내부'
                                AND LOWER(a.dtst_cd) = 'data675'

                            '''
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
        collect_data_list = []
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_bsc_info_stmt).all():
                    tn_data_bsc_info = TnDataBscInfo(**dict_row)

                    data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start)
                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + "_2024"

                    # th_data_clct_mastr_log set
                    th_data_clct_mastr_log = ThDataClctMastrLog()
                    th_data_clct_mastr_log.dtst_cd = tn_data_bsc_info.dtst_cd
                    th_data_clct_mastr_log.dtst_dtl_cd = tn_data_bsc_info.dtst_dtl_cd
                    th_data_clct_mastr_log.clct_ymd = data_interval_end.strftime("%Y%m%d")
                    th_data_clct_mastr_log.clct_data_nm = tn_data_bsc_info.dtst_nm
                    th_data_clct_mastr_log.data_crtr_pnttm = data_crtr_pnttm
                    th_data_clct_mastr_log.reclect_flfmt_nmtm = 0
                    th_data_clct_mastr_log.step_se_cd = CONST.STEP_CNTN
                    th_data_clct_mastr_log.stts_cd = CONST.STTS_WORK
                    th_data_clct_mastr_log.stts_dt = now(tz="UTC")
                    th_data_clct_mastr_log.stts_msg = CONST.MSG_CNTN_WORK
                    th_data_clct_mastr_log.crt_dt = now(tz="UTC")

                    # tn_clct_file_info 수집파일정보 set
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                    collect_data_list.append({
                                            "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                                            , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                            , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                            })
        except Exception as e:
            logging.info(f"insert_collect_data_info Exception::: {e}")
            raise e
        return collect_data_list

    @task_group(group_id='call_url_process')
    def call_url_process(collect_data_list):

        @task
        def create_directory(collect_data_list, **kwargs):
            """
            수집 파일 경로 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info
            return: file_path: tn_clct_file_info 테이블에 저장할 파일 경로
            """
            
            data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            #final_file_path = kwargs['var']['value'].storage_file_path # local test
            final_file_path = kwargs['var']['value'].final_file_path
            temp_list = []
            if isinstance(collect_data_list, list):  # list 인 경우
                temp_list.extend(collect_data_list)
            else:  # dict 인 경우
                temp_list.append(collect_data_list)
            for collect_data_dict in temp_list:
                tn_data_bsc_info = TnDataBscInfo(**collect_data_dict['tn_data_bsc_info'])

                # 파일 경로 설정
                file_path, full_file_path = CommonUtil.set_file_path(final_file_path, data_interval_end, tn_data_bsc_info)
            try:
                # 수집 폴더 경로 생성
                os.makedirs(full_file_path, exist_ok=True)
            except OSError as e:
                logging.info(f"create_directory OSError::: {e}")
                raise AirflowSkipException()
            logging.info(f"create_directory full_file_path::: {full_file_path}")
            return file_path
        
        @task
        def call_url(collect_data_list, file_path, **kwargs):
            """
            경로 내 파일 존재 확인 및 csv 파일 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
            return: file_size
            """
            import os
            from util.call_url_util import CallUrlUtil
            from util.file_util import FileUtil
            from xml_to_dict import XMLtoDict

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            # log_full_file_path = collect_data_list['log_full_file_path']
            esb_file_path = kwargs['var']['value'].esb_file_path  # 원천 파일 경로
            #final_file_path = kwargs['var']['value'].storage_file_path # local test
            final_file_path = kwargs['var']['value'].final_file_path

            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
            pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()

            # data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            data_interval_end = from_format(th_data_clct_mastr_log.clct_ymd,'YYYYMMDD')
            
            header = True   # 파일 헤더 모드
            mode = "w"  # 파일 쓰기 모드 overwrite
            page_no = 1  # 현재 페이지
            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
            source_file_name = None  # 원천 파일명
            full_file_path = final_file_path + file_path
            full_file_name = full_file_path + file_name
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr
            file_size = 0   # 파일 사이즈
            row_count = 0  # 행 개수
            # file_exist = False  # 파일 존재 여부

            # try:
            # 경로 내 파일 존재 확인
            # 초기구축용 - 파일명 뒤에 _날짜 를 데이터 기준 시점으로 설정
            for file in os.listdir(esb_file_path):
                # if file.startswith(data_interval_end.strftime("%Y%m%d")):
                source_file_name = file
                # file_exist = True
            # if file_exist == False:  # 파일 존재하지않을 때
            #     raise Exception(CONST.MSG_CLCT_COMP_NO_DATA)
            
                #with open(esb_file_path + source_file_name, 'r') as f: 
                with open(esb_file_path + source_file_name, 'r', encoding='utf-8', errors='replace') as f: #20241114
                    xml_content =   f.read()

                # XML 파일이 비어있는지 검사
                if not xml_content.strip():
                    logging.error(f"XML file {source_file_name} is empty.")
                    continue  # 다음 파일로 넘어가기
                    
                # NUL 문자 및 기타 제어 문자 제거
                #xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_content)
                xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F�]', '', xml_content)

                # 1. CDATA 섹션 닫힘 여부 확인 및 보정
                if xml_content.count('<![CDATA[') > xml_content.count(']]>'):
                    xml_content += ']]>'  # 닫힘 태그 추가

                # 2. reason 태그 열림-닫힘 불일치 수정
                if xml_content.count('<reason>') > xml_content.count('</reason>'):
                    print("Fixing <reason> tags...")
                    xml_content += '</reason>'

                # 모든 열리고 닫히지 않은 태그를 찾아 처리
                open_tags = re.findall(r'<([a-zA-Z_][^ />]*)[^>]*?>', xml_content)
                close_tags = re.findall(r'</([a-zA-Z_][^ />]*)>', xml_content)

                unmatched_open_tags = [tag for tag in open_tags if open_tags.count(tag) > close_tags.count(tag)]
                unmatched_close_tags = [tag for tag in close_tags if close_tags.count(tag) > open_tags.count(tag)]

                for tag in unmatched_open_tags:
                    logging.info(f"Fixing unmatched opening tag: <{tag}>")
                    xml_content += f"</{tag}>"

                for tag in unmatched_close_tags:
                    logging.info(f"Fixing unmatched closing tag: </{tag}>")
                    xml_content = f"<{tag}>" + xml_content

                # 유효성 검사: 비정상적인 종료 방지
                if not xml_content.strip().endswith('>'):
                    xml_content += '</root>'  # 루트 태그 보정

                # XML 파싱 시도
                try:
                    # 파싱 전에 XML 내용 로깅 (일부만 출력하여 확인)
                    logging.info(f"Parsing XML file {source_file_name}: {xml_content[:200]}...")  # 처음 200자만 로깅
                    json_data = XMLtoDict().parse(xml_content)
                except Exception as e:
                    logging.error(f"XML parsing error in file {source_file_name}: {e}")
                    raise e

                result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                result_json = result['result_json_array']

                dtst_se_val = {
                    "국민신문고_신문고민원_신청" : "Petition",
                    "국민신문고_신문고민원_접수" : "Receipt",
                    "국민신문고_신문고민원_처리" : "Process" }.get(th_data_clct_mastr_log.clct_data_nm)
                result_json = [item for item in result_json if item.get('dtst_se') == dtst_se_val]


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

                new_result_json = []
                for dict_value in result_json:
                    new_dict = {}

                    # dict_value의 키들을 소문자로 변환한 새로운 딕셔너리 생성
                    for key, value in dict_value.items():
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                new_key = f"{key}_{sub_key}".lower()
                                new_dict[new_key] = sub_value
                        else:
                            new_dict[key.lower()] = value

                    # 소문자로 변환한 딕셔너리로 누락된 컬럼을 체크하기 위해 lowercase_new_dict 생성
                    lowercase_new_dict = {key.lower(): value for key, value in new_dict.items()}

                    # dw_column_dict에서 누락된 컬럼을 new_dict에 추가
                    for missing_column in dw_column_dict:
                        if missing_column not in lowercase_new_dict and dtst_se_val != 'Process':
                            # update_column에 대해 적절한 대소문자 형태로 지정
                            update_column = {
                                "cancel_date": "Cancel_date",
                                "cancel_datetime": "Cancel_dateTime",
                                "cancel_reason": "Cancel_reason",
                                "expand_title": "Expand_title",
                                "expand_reason": "Expand_reason",
                                "satisfy_content": "Satisfy_Content",
                                "satisfy_moreword": "Satisfy_moreWord",
                                "satisfy_regdate": "Satisfy_regDate",
                                "satisfy_regdatetime": "Satisfy_regDateTime"
                            }.get(missing_column, missing_column)  # 매핑이 없으면 기본적으로 소문자로 추가

                            new_dict[update_column] = None  # 누락된 컬럼을 None으로 추가

                    # 누락된 컬럼이 모두 추가된 new_dict를 new_result_json에 포함
                    new_result_json.append(new_dict)

                result_size = len(new_result_json)

                # 신문고민원_처리 데이터 비식별 처리
                if dtst_se_val == 'Process':
                    for item in new_result_json:
                        item['dutyId'] = CallUrlUtil.anonymize(item.get('dutyId', ''))
                        item['dutyName'] = CallUrlUtil.anonymize(item.get('dutyName', ''))

                # add_column_dict = {}
                # missing_columns = ""
                
                # for dict_value in result_json:
                #     # 컬럼 존재하지않는 경우 예외 처리
                #     if th_data_clct_mastr_log.clct_data_nm == "국민신문고_신문고민원_신청":
                #         missing_columns = "Cancel_date,Cancel_dateTime,Cancel_reason"
                #     if th_data_clct_mastr_log.clct_data_nm == "국민신문고_신문고민원_접수":
                #         missing_columns = "Expand_title,Expand_reason,Satisfy_Content,Satisfy_moreWord,Satisfy_regDate,Satisfy_regDateTime"
                #     if missing_columns != "":
                #         for missing_column in missing_columns.split(','):
                #             if missing_column not in dict_value.keys():
                #                 add_column_dict.update({missing_column: None})
                #     dict_value.update(add_column_dict)
                # result_size = len(result_json)

                sf = source_file_name.split('_')
                if len(sf[-1].split('.')[0]) == 8:
                    data_crtr_pnttm = sf[-1].split('.')[0]
                else:
                    data_crtr_pnttm = from_format(source_file_name[:8],'YYYYMMDD').add(days=-1).strftime("%Y%m%d")

                # 데이터 존재 시
                if result_size != 0:
                    # csv 파일 생성
                    CallUrlUtil.create_csv_file(link_file_sprtr, th_data_clct_mastr_log.data_crtr_pnttm, th_data_clct_mastr_log.clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no)

                row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                if row_count != 0:
                    header = False
                    mode = "a"
                
                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    file_size = os.path.getsize(full_file_name)
                logging.info(f"call_url file_size::: {file_size}")

                logging.info(f"수집 끝")
            #     if row_count == 0:
            #         CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "y")
            #         raise AirflowSkipException()
            #     else:
            #         # tn_clct_file_info 수집파일정보
            #         tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, tn_clct_file_info.insd_file_nm, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                    
            #         CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP, "y")
            #         if link_file_crt_yn == "y":
            #             CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
            #         CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_INT, "y")
            # except Exception as e:
            #     CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "y")
            #     logging.error(f"call_url Exception::: {e}")
            #     raise e
    
        file_path = create_directory(collect_data_list)
        file_path >> call_url(collect_data_list, file_path)
    
    collect_data_list = insert_collect_data_info()
    call_url_process.expand(collect_data_list = collect_data_list)
    
dag_object = xml_to_csv_esb_fail_retry()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024,1,11,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
