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

# 20250301~0531까지 재수집 csv생성하는 소스
# csv가 3개 (신청/처리/접수) 생성되는데, 한 파일 안에 data_crtr_pnttm 동일하게 나옴
# csv 파일 자체로 수동 수집할때 사용 (초기데이터 구축할때도 사용)

@dag(
    dag_id="sdag_xml_to_csv_esb_only1",
    schedule="30 2 * * *",
    start_date=datetime(2025, 3, 1, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["xml_to_csv_only1", "day", "int"],
)
def xml_to_csv_esb_reprocess():

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
                            SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                            FROM tn_data_bsc_info
                            WHERE 1=1
                                AND LOWER(link_yn) = 'y'
                                AND LOWER(clct_yn) = 'y'
                                AND LOWER(link_clct_mthd_dtl_cd) = 'tcp'
                                AND LOWER(link_clct_cycle_cd) = 'day'
                                AND link_ntwk_otsd_insd_se = '내부'
                                AND LOWER(dtst_cd) = 'data675'
                                and lower(dtst_dtl_cd) != 'data675_1'
                            '''
        # data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        # data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정

        # 수집 대상 날짜 범위 제한 (2025-03-01 ~ 2025-05-31)
        #20250618
        data_interval_start = datetime(2025, 3, 1, tz="Asia/Seoul")
        data_interval_end = datetime(2025, 5, 31, tz="Asia/Seoul")

        logging.info(f"재수집 기간: {data_interval_start.strftime('%Y-%m-%d')} ~ {data_interval_end.strftime('%Y-%m-%d')}")

        collect_data_list = []
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_bsc_info_stmt).all():
                    tn_data_bsc_info = TnDataBscInfo(**dict_row)

                    data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start)
                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + "_reprocess_202503_202505"
                    logging.info(f"for dict_row in conn__안에서 찍히는거?__file_name::: {file_name}")

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
                logging.info(f"for dict_row in conn__밖에서 찍히는거?__file_name::: {file_name}")
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
            
            # data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")  # 실제 실행하는 날짜를 KST 로 설정
            data_interval_end = datetime(2025, 5, 31, tz="Asia/Seoul")
            # final_file_path = kwargs['var']['value'].final_file_path
            final_file_path = kwargs['var']['value'].root_final_file_path   #local test

            logging.info(f"create_directory___data_interval_end::: {data_interval_end}")
            logging.info(f"create_directory___final_file_path::: {final_file_path}")

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
            import re
            import shutil
            from util.call_url_util import CallUrlUtil
            from util.file_util import FileUtil
            from xml_to_dict import XMLtoDict
            from datetime import datetime, timedelta

            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            # log_full_file_path = collect_data_list['log_full_file_path']
            # esb_file_path = kwargs['var']['value'].esb_file_path  # 원천 파일 경로
            esb_file_path = kwargs['var']['value'].root_esb_file_path  # local test
            # final_file_path = kwargs['var']['value'].final_file_path
            root_final_file_path = kwargs['var']['value'].root_final_file_path  # local test
            # error_file_path = "/home/gsdpmng/data/error_file"  # 에러 파일 경로

            # 에러 파일 저장 경로 생성
            # os.makedirs(error_file_path, exist_ok=True)
            
            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
            pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()
            
            #20250618
            # 처리할 기간 설정 (2025.03.01 ~ 2025.05.31)
            start_date = datetime(2025, 3, 1)
            end_date = datetime(2025, 5, 31)
            
            header = True   # 파일 헤더 모드
            mode = "w"  # 파일 쓰기 모드 overwrite
            page_no = 1  # 현재 페이지
            # link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
            source_file_name = None  # 원천 파일명
            # full_file_path = final_file_path + file_path
            full_file_path = root_final_file_path + file_path   # local test
            full_file_name = full_file_path + file_name
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr

            #20250618
            total_file_size = 0
            total_row_count = 0
            processed_files = []
            total_result_count = 0

            try:
                # 처리 대상 날짜 범위 생성
                current_date = start_date
                target_dates = []
                
                while current_date <= end_date:
                    target_dates.append(current_date.strftime("%Y%m%d"))
                    current_date += timedelta(days=1)
                logging.info(f"처리 대상 날짜 범위: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
                logging.info(f"총 {len(target_dates)}개 날짜 대상")
            
            # finally:
            #     logging.info("작업이 완료되었습니다.")      

                try:
                    all_files = os.listdir(esb_file_path)
                    logging.info(f"esb_file_path 내 전체 파일 개수: {len(all_files)}")
                except Exception as e:
                    logging.error(f"esb_file_path 읽기 실패: {e}")
                    raise Exception("원천 파일 경로를 읽을 수 없습니다.")

                # 재수집 파일 패턴에 맞는 파일들 찾기
                # 패턴: 숫자_YYYYMMDD.xml (예: 12341241_20250301.xml)
                reprocess_pattern = re.compile(r'.*_(\d{8})\.xml$')
                matching_files = []

                logging.info("파일 패턴 매칭 시작...")
                for file in all_files:
                    match = reprocess_pattern.match(file)
                    if match:
                        file_date = match.group(1)  # YYYYMMDD 추출
                        logging.info(f"패턴 매칭된 파일: {file}, 추출된 날짜: {file_date}")
                        if file_date in target_dates:
                            matching_files.append((file, file_date))
                            logging.info(f"대상 기간에 포함된 파일: {file} (날짜: {file_date})")
                    else:
                        # 매칭되지 않은 파일도 로깅 (처음 몇 개만)
                        if len([f for f in all_files if not reprocess_pattern.match(f)]) <= 5:
                            logging.info(f"패턴 매칭되지 않은 파일: {file}")
                            
                logging.info(f"재수집 패턴에 맞는 파일 개수: {len(matching_files)}")
                
                if not matching_files:
                    logging.warning("재수집 패턴에 맞는 파일이 없습니다.")
                    raise AirflowSkipException("재수집 대상 파일 없음")

                # 날짜순으로 정렬
                matching_files.sort(key=lambda x: x[1])
                # DB 컬럼 정보 조회 (한 번만 실행)
                get_data_column_stmt = f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{tn_data_bsc_info.dw_tbl_phys_nm}'
                    AND column_name NOT IN ('data_crtr_pnttm','clct_sn','clct_pnttm','clct_log_sn','page_no')
                    ORDER BY ordinal_position
                """
                
                with session.begin() as conn:
                    dw_column_dict = [dict_row[0] for dict_row in conn.execute(get_data_column_stmt).all()]

                # 각 파일 처리
                is_first_file = True
                
                for source_file_name, file_date in matching_files:
                    logging.info(f"처리 중인 파일: {source_file_name} (날짜: {file_date})")
                    
                    try:
                        # 파일 읽기 (다중 인코딩 시도)
                        xml_content = None
                        encodings = ['utf-8', 'euc-kr', 'cp949']
                        
                        for encoding in encodings:
                            try:
                                source_file_full_path = os.path.join(esb_file_path, source_file_name)
                                
                                with open(source_file_full_path, 'r', encoding=encoding) as f:
                                    xml_content = f.read()
                                logging.info(f"파일을 성공적으로 읽었습니다. 인코딩: {encoding}")
                                break
                            except UnicodeDecodeError:
                                logging.warning(f"Encoding {encoding} failed for file {source_file_name}. Trying next...")
                            except FileNotFoundError:
                                logging.error(f"File not found: {source_file_full_path}")
                                break
                            except Exception as e:
                                logging.error(f"Unexpected error while reading file {source_file_name}: {e}")
                                break

                        if xml_content is None:
                            logging.error(f"XML 내용을 읽지 못했습니다: {source_file_name}")
                            continue

                        # XML 파싱
                        try:
                            json_data = XMLtoDict().parse(xml_content)
                            logging.info(f"구문 분석 성공: {source_file_name}")
                        except Exception as e:
                            logging.error(f"Error parsing XML file {source_file_name}: {e}")
                            continue

                        # JSON 데이터 처리
                        result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                        result_json = result['result_json_array']

                        # 데이터 필터링
                        dtst_se_val = {
                            "국민신문고_신문고민원_신청": "Petition",
                            "국민신문고_신문고민원_접수": "Receipt",
                            "국민신문고_신문고민원_처리": "Process"
                        }.get(th_data_clct_mastr_log.clct_data_nm)
                        
                        if dtst_se_val:
                            result_json = [item for item in result_json if item.get('dtst_se') == dtst_se_val]

                        # 컬럼 매핑 및 누락 컬럼 추가
                        new_result_json = []
                        for dict_value in result_json:
                            new_dict = {}
                            
                            # 중첩된 딕셔너리 평탄화
                            for key, value in dict_value.items():
                                if isinstance(value, dict):
                                    for sub_key, sub_value in value.items():
                                        new_key = f'{key}_{sub_key}'
                                        new_dict[new_key] = sub_value
                                else:
                                    new_dict[key] = value

                            # 누락 컬럼 처리
                            lowercase_new_dict = {key.lower(): value for key, value in new_dict.items()}
                            
                            for missing_column in dw_column_dict:
                                if missing_column not in lowercase_new_dict and dtst_se_val != 'Process':
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
                                    }.get(missing_column, missing_column)
                                    
                                    new_dict[update_column] = None

                            new_result_json.append(new_dict)

                        result_size = len(new_result_json)
                        logging.info(f"파일 {source_file_name}의 JSON 데이터 크기: {result_size}")

                        if result_size == 0:
                            logging.warning(f"파일 {source_file_name}: 처리할 데이터가 없습니다.")
                            continue

                        # 데이터 비식별 처리
                        if dtst_se_val == 'Process':
                            for item in new_result_json:
                                item['dutyId'] = CallUrlUtil.anonymize(item.get('dutyId', ''))
                                item['dutyName'] = CallUrlUtil.anonymize(item.get('dutyName', ''))

                        # 신문고민원_신청 데이터 비식별 처리 (cellPhone    linePhone    birthDate    sex)
                        if dtst_se_val == 'Petition':
                            for item in new_result_json:
                                item['Petitioner_cellPhone'] = CallUrlUtil.anonymize(item.get('Petitioner_cellPhone', ''))
                                item['Petitioner_linePhone'] = CallUrlUtil.anonymize(item.get('Petitioner_linePhone', ''))
                                item['Petitioner_birthDate'] = CallUrlUtil.anonymize(item.get('Petitioner_birthDate', ''))
                                item['Petitioner_sex'] = CallUrlUtil.anonymize(item.get('Petitioner_sex', ''))

                        # 파일별 data_crtr_pnttm 설정 (파일 날짜 기준)
                        file_data_crtr_pnttm = file_date  # YYYYMMDD 형태     

                        # 각 파일별로 clct_sn을 1부터 시작하여 데이터 준비
                        processed_data = []

                        for item in new_result_json:
                            # 기존 데이터에서 모든 메타 컬럼 제거
                            clean_item = {k: v for k, v in item.items() 
                                        if k not in ['clct_sn', 'data_crtr_pnttm', 'clct_pnttm', 'clct_log_sn', 'page_no']}
                            processed_data.append(clean_item)
                        
                        # CSV 파일 직접 생성 (한글 인코딩 문제 해결)
                        import csv
                        
                        if is_first_file:
                            # 첫 번째 파일: 헤더와 함께 새로 생성
                            with open(full_file_name, 'w', newline='', encoding='utf-8-sig') as csvfile:
                                if processed_data:
                                    # 컬럼 순서: clct_sn + 실제데이터 + 메타컬럼들
                                    data_fields = list(processed_data[0].keys())  # 실제 데이터 컬럼들
                                    fieldnames = ['clct_sn'] + data_fields + ['data_crtr_pnttm', 'clct_pnttm', 'clct_log_sn', 'page_no']
                                    
                                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=link_file_sprtr)
                                    writer.writeheader()
                                    
                                    # 각 행에 clct_sn과 메타 컬럼들 추가하여 쓰기
                                    for idx, row in enumerate(processed_data, 1):
                                        complete_row = {
                                            'clct_sn': idx,  # 파일별로 1부터 시작
                                            **row,  # 실제 데이터
                                            'data_crtr_pnttm': file_data_crtr_pnttm,  # 파일별 날짜
                                            'clct_pnttm': now(tz="UTC").strftime("%Y%m%d%H%M%S"),
                                            'clct_log_sn': th_data_clct_mastr_log.clct_log_sn,
                                            'page_no': 1
                                        }
                                        writer.writerow(complete_row)
                        else:
                            # 이후 파일들: 헤더 없이 append
                            with open(full_file_name, 'a', newline='', encoding='utf-8-sig') as csvfile:
                                if processed_data:
                                    # 동일한 컬럼 순서 사용
                                    data_fields = list(processed_data[0].keys())  # 실제 데이터 컬럼들
                                    fieldnames = ['clct_sn'] + data_fields + ['data_crtr_pnttm', 'clct_pnttm', 'clct_log_sn', 'page_no']
                                    
                                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=link_file_sprtr)
                                    
                                    # 각 행에 clct_sn과 메타 컬럼들 추가하여 쓰기
                                    for idx, row in enumerate(processed_data, 1):
                                        complete_row = {
                                            'clct_sn': idx,  # 파일별로 1부터 시작
                                            **row,  # 실제 데이터
                                            'data_crtr_pnttm': file_data_crtr_pnttm,  # 파일별 날짜
                                            'clct_pnttm': now(tz="UTC").strftime("%Y%m%d%H%M%S"),
                                            'clct_log_sn': th_data_clct_mastr_log.clct_log_sn,
                                            'page_no': 1
                                        }
                                        writer.writerow(complete_row)
                        
                        processed_files.append(source_file_name)
                        total_result_count += result_size
                        is_first_file = False
                        
                        logging.info(f"파일 {source_file_name} 처리 완료. 데이터 건수: {result_size}")

                    except Exception as e:
                        logging.error(f"파일 {source_file_name} 처리 중 오류: {e}")
                        continue

                # 최종 결과 확인
                if os.path.exists(full_file_name):
                    total_file_size = os.path.getsize(full_file_name)
                    total_row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)
                    
                logging.info(f"=== 재수집 완료 ===")
                logging.info(f"처리된 파일 개수: {len(processed_files)}")
                logging.info(f"총 데이터 건수: {total_result_count}")
                logging.info(f"최종 CSV 파일 크기: {total_file_size} bytes")
                logging.info(f"최종 CSV 행 개수: {total_row_count}")
                logging.info(f"처리된 파일 목록: {processed_files}")

                if total_row_count == 0:
                    logging.warning("최종 CSV 파일에 데이터가 없습니다.")
                    raise AirflowSkipException("생성된 데이터 없음")

                # 파일 정보 업데이트 (필요한 경우)
                tn_clct_file_info = CommonUtil.set_file_info(
                    TnClctFileInfo(), 
                    th_data_clct_mastr_log, 
                    tn_clct_file_info.insd_file_nm, 
                    file_path, 
                    tn_data_bsc_info.link_file_extn, 
                    total_file_size, 
                    None
                )

                return {
                    "file_size": total_file_size,
                    "row_count": total_row_count,
                    "processed_files": processed_files,
                    "total_result_count": total_result_count
                }

            except AirflowSkipException:
                raise
            except Exception as e:
                logging.error(f"call_url Exception: {e}")
                raise e

        file_path = create_directory(collect_data_list)
        result = call_url(collect_data_list, file_path)
        return result
    
    collect_data_list = insert_collect_data_info()
    call_url_process.expand(collect_data_list=collect_data_list)
    
dag_object = xml_to_csv_esb_reprocess()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2025,6,11,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )
