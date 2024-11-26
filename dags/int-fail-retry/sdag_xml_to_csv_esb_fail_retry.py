import logging
import urllib3
import os

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
    dag_id="sdag_xml_to_csv_esb_fail_retry",
    schedule="30 2 * * *",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["xml_to_csv", "day", "int"],
)
def xml_to_csv_esb_fail_retry():

    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

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
        select_log_info_stmt = f'''
                            SELECT b.*
                            FROM tn_data_bsc_info a, th_data_clct_mastr_log b
                            WHERE 1=1
                                AND a.dtst_cd = b.dtst_cd
                                AND LOWER(link_yn) = 'y'
                                AND LOWER(clct_yn) = 'y'
                                AND LOWER(link_clct_mthd_dtl_cd) = 'tcp'
                                AND LOWER(link_clct_cycle_cd) = 'day'
                                AND link_ntwk_otsd_insd_se = '내부'
                                AND LOWER(a.dtst_cd) = 'data675'
                                AND LOWER(b.step_se_cd) NOT IN ('{CONST.STEP_FILE_STRGE_SEND}', '{CONST.STEP_DW_LDADNG}') -- 스토리지파일전송단계, DW 적재단계 제외
                                AND COALESCE(stts_msg, '') != '{CONST.MSG_CLCT_COMP_NO_DATA}' -- 원천데이터 없음 제외
                                AND NOT (LOWER(b.step_se_cd) = '{CONST.STEP_FILE_INSD_SEND}' AND LOWER(b.stts_cd) = '{CONST.STTS_COMP}') -- 내부파일전송 성공 제외
                                {run_conf}
                            ORDER BY b.clct_log_sn
                            '''
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

        @task
        def create_directory(collect_data_list, **kwargs):
            """
            수집 파일 경로 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info
            return: file_path: tn_clct_file_info 테이블에 저장할 파일 경로
            """
            
            data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            # final_file_path = kwargs['var']['value'].final_file_path
            final_file_path = kwargs['var']['value'].root_final_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, data_interval_end, final_file_path, "n")
            return file_path
        
        @task
        def call_url(collect_data_list, file_path, **kwargs):
            """
            경로 내 파일 존재 확인 및 csv 파일 생성
            params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
            return: file_size
            """
            import os
            import shutil
            from util.call_url_util import CallUrlUtil
            from util.file_util import FileUtil
            from xml_to_dict import XMLtoDict

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']
            esb_file_path = kwargs['var']['value'].esb_file_path  # 원천 파일 경로
            # final_file_path = kwargs['var']['value'].final_file_path
            final_file_path = kwargs['var']['value'].root_final_file_path
            # error_file_path = "/home/gsdpmng/data/error_file"  # 에러 파일 경로
            # error_file_path = kwargs['var']['value'].error_file_path  # 에러 파일 경로

            # 에러 파일 저장 경로 생성
            # os.makedirs(error_file_path, exist_ok=True)
            
            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
            pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()

            data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            # data_interval_end = from_format(th_data_clct_mastr_log.clct_ymd,'YYYYMMDD')
            
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
            file_exist = False  # 파일 존재 여부

            try:
                for file in os.listdir(esb_file_path):
                    print("비교하는 file명 : "+file)
                    print("실행하는 날짜 확인 : "+data_interval_end.strftime("%Y%m%d"))
                    if (file.startswith(data_interval_end.strftime("%Y%m%d"))):
                        source_file_name = file
                        # source_file_full_path = os.path.join(esb_file_path, source_file_name)
                        # error_file_full_path = os.path.join(error_file_path, source_file_name)
                        file_exist = True
                        logging.info(f"파일 발견: {source_file_name}")
                        break

                if file_exist == False:  # 파일 존재하지않을 때
                    logging.error("파일이 존재하지 않습니다.")
                    raise Exception(CONST.MSG_CLCT_COMP_NO_DATA)
                
                    # source_file_name = file
                    # source_file_full_path = os.path.join(esb_file_path, source_file_name)
                    # error_file_full_path = os.path.join(error_file_path, source_file_name)

                # 가능한 인코딩 목록 정의
                xml_content = None
                encodings = ['utf-8', 'euc-kr', 'cp949']
                
                for encoding in encodings:
                    try:
                        logging.info(f"Source file path: {os.path.join(esb_file_path, source_file_name)}")
                        logging.info(f"Does source file exist: {os.path.exists(os.path.join(esb_file_path, source_file_name))}")
    
                        # 파일을 시도된 인코딩으로 읽음
                        with open(os.path.join(esb_file_path, source_file_name), 'r', encoding=encoding) as f:
                            xml_content = f.read()
                        logging.info(f"파일을 성공적으로 읽었습니다. 인코딩: {encoding}")
                        logging.info(f"sssss First 300 characters of XML content: {xml_content[:300]}")                            
                        break  # 성공적으로 읽었다면 루프 종료
                    except UnicodeDecodeError:
                        logging.warning(f"Encoding {encoding} failed for file {source_file_name}. Trying next...")
                    except FileNotFoundError:
                        logging.error(f"File not found: {os.path.join(esb_file_path, source_file_name)}")
                        break
                    except Exception as e:
                        logging.error(f"Unexpected error while reading file {source_file_name}: {e}")
                        break

                # 파일 읽기가 실패한 경우 건너뛰기
                if xml_content is None:
                    logging.error("XML 내용을 읽지 못했습니다.")
                    raise Exception("파일 읽기 실패")
                    # try:
                    #     shutil.move(source_file_full_path, error_file_full_path)
                    #     logging.warning(f"Moved failed file {source_file_name} to error directory {error_file_path}.")
                    # except Exception as e:
                    #     logging.error(f"Failed to move file {source_file_name} to error directory: {e}")
                    

                # XML 내용 일부를 로깅 (xml_content가 None이 아닌 경우에만)
                # logging.info(f"First 300 characters of XML content in {source_file_name}: {xml_content[:300]}")

                # XML 파싱
                try:
                    json_data = XMLtoDict().parse(xml_content)
                    logging.info(f"구문 분석 성공한 xml 파일 : {source_file_name}")

                    # JSON 데이터 확인
                    logging.info(f"JSON 변환 성공. 첫 300자: {str(json_data)[:300]}")
                except Exception as e:
                    logging.error(f"Error parsing XML file {source_file_name}: {e}")
                    raise

                result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                result_json = result['result_json_array']

                logging.info(f"dtst_nm: {tn_data_bsc_info.dtst_nm}")
                logging.info(f"clct_data_nm: {th_data_clct_mastr_log.clct_data_nm}")

                dtst_se_val = {
                    "국민신문고_신문고민원_신청" : "Petition",
                    "국민신문고_신문고민원_접수" : "Receipt",
                    "국민신문고_신문고민원_처리" : "Process" }.get(th_data_clct_mastr_log.clct_data_nm)
                
                result_json = [item for item in result_json if item.get('dtst_se') == dtst_se_val]
                
                # # JSON 데이터 필터링
                # dtst_se_val_normalized = dtst_se_val.lower()
                # filtered_result_json = [
                #     item for item in result_json if item.get('dtst_se', '').strip().lower() == dtst_se_val_normalized
                # ]

                # # 필터링된 결과 확인
                # if not filtered_result_json:
                #     logging.error(f"No matching data for dtst_se_val: {dtst_se_val_normalized}. Check source JSON.")
                #     logging.info(f"Full JSON data: {result_json[:5]}")  # 디버깅용
                #     raise ValueError(f"No matching data for dtst_se_val: {dtst_se_val_normalized}")
                # else:
                #     logging.info(f"Filtered JSON data: {filtered_result_json[:3]}")

            # 컬럼 존재하지않는 경우 예외 처리
                get_data_column_stmt = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{tn_data_bsc_info.dw_tbl_phys_nm}'
                    AND column_name NOT IN ('data_crtr_pnttm','clct_sn','clct_pnttm','clct_log_sn','page_no')
                    ORDER BY ordinal_position
                """
                # json 결과 값이랑 db 컬럼이랑 비교해서 없는 컬럼들 데이터값은 None값으로 한 걸 추가해서 new json result 값으로 만든다음에 csv 로 만듦.
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
                                new_key = f'{key}_{sub_key}'
                                new_dict[new_key] = sub_value
                        else:
                            new_dict[key] = value

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
                logging.info(f"최종 JSON 데이터 크기 (CSV 변환용): {result_size}")

                if result_size > 0:
                    # JSON 데이터의 첫 3개 로깅
                    logging.info(f"First 3 entries of new_result_json: {new_result_json[:3]}")
                else:
                    logging.warning("new_result_json is empty after filtering.")

                # 신문고민원_처리 데이터 비식별 처리
                if dtst_se_val == 'Process':
                    for item in new_result_json:
                        item['dutyId'] = CallUrlUtil.anonymize(item.get('dutyId', ''))
                        item['dutyName'] = CallUrlUtil.anonymize(item.get('dutyName', ''))

                # 데이터 존재 시
                if result_size != 0:
                    # csv 파일 생성
                    CallUrlUtil.create_csv_file(link_file_sprtr, th_data_clct_mastr_log.data_crtr_pnttm, th_data_clct_mastr_log.clct_log_sn, full_file_path, file_name, new_result_json, header, mode, page_no)

                row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                if row_count != 0:
                    logging.info(f"현재까지 파일 내 행 개수: {row_count}")
                
                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    file_size = os.path.getsize(full_file_name)
                logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")

                # logging.info(f"수집 끝")
                if row_count == 0:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "n")
                    raise AirflowSkipException()
                else:
                    # tn_clct_file_info 수집파일정보
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, tn_clct_file_info.insd_file_nm, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                    
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP, "n")
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_COMP, CONST.MSG_FILE_INSD_SEND_COMP_INT, "n")
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
                logging.error(f"call_url Exception::: {e}")
                raise e
    
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
        execution_date=datetime(2024,11,21,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
