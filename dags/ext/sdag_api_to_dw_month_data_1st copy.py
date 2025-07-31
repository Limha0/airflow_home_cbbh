import logging
import os
import urllib3
import sys

from pendulum import datetime, from_format,now
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.tn_clct_file_info import TnClctFileInfo
from util.file_util import FileUtil
from util.common_util import CommonUtil
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST

from util.date_custom_util import DateUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.tdm_list_url_info import TdmListUrlInfo
from dto.tdm_file_url_info import TdmFileUrlInfo
from dto.tdm_standard_url_info import TdmStandardUrlInfo
from airflow.exceptions import AirflowSkipException

def call_additional_api_and_save_csv(row, output_dir, null_atchfile_count, valid_atchfile_count, api_error_count):
    """
    추가 API 호출하여 파일 다운로드 및 CSV 저장
    """
    import re
    import json
    import pandas as pd
    import os
    from io import BytesIO
    from xml_to_dict import XMLtoDict
    import requests

    list_id = row.get('list_id')
    id_ = row.get('id')
    title = row.get('title') or f"{list_id}_{id_}"

    if not list_id or not id_:
        logging.warning(f"⚠️ list_id 또는 id 없음 → 생략됨: {row}")
        return null_atchfile_count, valid_atchfile_count, api_error_count

    new_url = f"https://www.data.go.kr/tcs/dss/selectFileDataDownload.do?recommendDataYn=Y&publicDataPk={list_id}&publicDataDetailPk={id_}"
    try:
        resp = requests.get(new_url, verify=False)
        data = resp.json()
        file_info = data.get('fileDataRegistVO') or {}

        atchFileId = file_info.get('atchFileId')
        fileDetailSn = file_info.get('fileDetailSn')
        dataNm = file_info.get('dataNm')
        orginlFileNm = file_info.get('orginlFileNm') or ''

        if not all([atchFileId, fileDetailSn, dataNm]):
            null_atchfile_count[0] += 1
            logging.warning(f"⚠️ 메타정보 누락: {title},{list_id},{id_}")
            logging.info(f"⚠️ 메타정보 누락 데이터 확인 : {atchFileId},{fileDetailSn},{dataNm}")
            return null_atchfile_count, valid_atchfile_count ,api_error_count

        safe_dataNm = re.sub(r'[^\w\-.]', '_', str(dataNm))
        base_file_name = os.path.splitext(orginlFileNm)[0]
        _, ext = os.path.splitext(orginlFileNm.lower())
        raw_path = os.path.join(output_dir, f"{safe_dataNm}{ext}")
        csv_path = os.path.join(output_dir, f"{safe_dataNm}.csv")

        download_url = f"https://www.data.go.kr/cmm/cmm/fileDownload.do?atchFileId={atchFileId}&fileDetailSn={fileDetailSn}&dataNm={dataNm}"
        download_resp = requests.get(download_url, verify=False)

        if download_resp.status_code != 200:
            null_atchfile_count[0] += 1
            logging.warning(f"❌ 다운로드 실패: {download_url}, status={download_resp.status_code}")
            return null_atchfile_count, valid_atchfile_count, api_error_count

        # 원본 파일 저장
        with open(raw_path, 'wb') as f:
            f.write(download_resp.content)
        logging.info(f"✅ 원본 저장 완료: {raw_path} ({len(download_resp.content)} bytes)")
        valid_atchfile_count[0] += 1

        # CSV 저장 로직
        if ext == ".csv":
            try:
                # 인코딩 감지 및 디코딩 시도
                try:
                    decoded_text = download_resp.content.decode("utf-8-sig")
                except UnicodeDecodeError:
                    decoded_text = download_resp.content.decode("cp949")

                with open(csv_path, 'w', encoding='utf-8-sig') as f:
                    f.write(decoded_text)
                logging.info(f"✅ CSV 텍스트 저장 완료: {csv_path}")
            except Exception as e:
                null_atchfile_count[0] += 1
                logging.warning(f"❌ CSV 텍스트 저장 실패: {e}")

        elif ext == ".xml":
            try:
                xml_dict = XMLtoDict().parse(download_resp.text)
                root_key = next(iter(xml_dict))
                # xml_dict = {'root': {'header': {...}, 'Row': [...]}},  root_key = 'root'
                content = xml_dict[root_key]

                # row가 하나일때 리스트로 변환해서 csv로 저장
                # Row 노드가 없으면 첫 번째 리스트나 딕셔너리 추출
                row_data = None
                if "Row" in content:
                    row_data = content["Row"]
                else:
                    row_data = next((v for k, v in content.items() if isinstance(v, list) or isinstance(v, dict)), None)

                # 단일 dict일 경우 list로 감싸기
                if isinstance(row_data, dict):
                    row_data = [row_data]
                elif not isinstance(row_data, list):
                    row_data = []
                    

                df = pd.json_normalize(row_data)
                if df.empty:
                    logging.warning("⚠ XML 파싱 결과 비어 있음")
                else:
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    logging.info(f"✅ XML ➜ CSV 저장 완료: {csv_path}")
            except Exception as e:
                null_atchfile_count[0] += 1
                logging.warning(f"❌ XML ➜ CSV 변환 실패: {e}")

        elif ext in [".xlsx", ".xls"]:
            # try:
            #     # XLSX 파일은 시각 요소가 포함된 문서로 판단되어
            #     # CSV 변환 생략하고 원본만 유지
            #     logging.info(f"✅ XLSX 파일은 시각 요소가 포함된 문서로 판단되어 변환 생략: {raw_path}")
            # except Exception as e:
            #     null_atchfile_count[0] += 1
            #     logging.warning(f"❌ XLSX ➜ CSV 변환 실패: {e}")

            # XLSX 파일을 pandas로 읽어 CSV로 변환
            # pandas로 읽을 수 없는 경우가 있어 예외 처리 필요 => CSV 변환 생략하고 원본만 유지
        
            try:
                from io import BytesIO
                import pandas as pd

                excel_file = BytesIO(download_resp.content)
                xls = pd.ExcelFile(excel_file)
                sheet = xls.sheet_names[0]
                df = xls.parse(sheet)

                # 정형 데이터가 실제 존재할 경우만 CSV 변환
                if df.empty or df.columns.size == 0:
                    logging.info(f"⚠ 시각화 위주의 XLSX로 판단되어 CSV 변환 생략: {raw_path}")
                else:
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    logging.info(f"✅ XLSX ➜ CSV 저장 완료: {csv_path}")
                    valid_atchfile_count[0] += 1

            except Exception as e:
                null_atchfile_count[0] += 1
                logging.warning(f"❌ XLSX ➜ CSV 변환 실패: {e}")

        else:
            logging.warning(f"⚠ 지원되지 않는 확장자: {ext} - 원본만 저장됨")

    except json.JSONDecodeError:
        null_atchfile_count[0] += 1
        logging.warning(f"❌ 메타정보 JSON 파싱 실패: {new_url}")
    except Exception as e:
        api_error_count[0] += 1
        logging.warning(f"❌ 추가 API 호출 실패: {e}")
    
    return null_atchfile_count, valid_atchfile_count, api_error_count

@dag(
    dag_id="sdag_api_dw_month_data_1st",
    schedule="@monthly",
    start_date=datetime(2025, 7, 28, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
    catchup=False,
    # render Jinja template as native Python object
    render_template_as_native_obj=True,
    tags=["api_to_csv", "month", "ext","data"],
)
def api_dw_month_data_1st():
    # PostgresHook 객체 생성
    pg_hook = PostgresHook(postgres_conn_id='gsdpdb_db_conn')

    #sqlalchemy 를 이용한 connection
    engine = pg_hook.get_sqlalchemy_engine()

    # sqlalchey session 생성
    session = sessionmaker(engine, expire_on_commit=False)

    @task
    def collect_data_info(**kwargs): # 수집 데이터 정보 조회
        """
        tn_data_bsc_info테이블에서 수집 대상 기본 정보 조회 후 th_data_clct_mastr_log 테이블에 입력
        return: collect_data_list
        """
        print("hello")
        # print("kwargs :" , kwargs)
        select_bsc_info_stmt = '''
                                select *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                                from tn_data_bsc_info
                                where
                                    use_yn = 'y'
                                and data_rls_se_cd = 'un_othbc'
                                and pvdr_site_cd = 'ps00005'
                                and dtst_cd not in (
                                'data919'
                                ,'data920'
                                ,'data922'    
                                )
                                and dtst_cd in ('data10006','data10018','data10030')--증평군
                                order by sn
                            '''
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")   # 실제 실행하는 날짜를 KST 로 설정
        # collect_data_list = []
        collect_data_list = CommonUtil.insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs)
        collect_data_list = []
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_bsc_info_stmt).all():
                    tn_data_bsc_info = TnDataBscInfo(**dict_row)

                    data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start)
                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + "20250728"

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
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            temp_list = []
            if isinstance(collect_data_list, list):  # list 인 경우
                temp_list.extend(collect_data_list)
            else:  # dict 인 경우
                temp_list.append(collect_data_list)
            for collect_data_dict in temp_list:
                tn_data_bsc_info = TnDataBscInfo(**collect_data_dict['tn_data_bsc_info'])

                # 파일 경로 설정
                file_path, full_file_path = CommonUtil.set_file_path(root_collect_file_path, data_interval_end, tn_data_bsc_info)
            try:
                # 수집 폴더 경로 생성
                os.makedirs(full_file_path, exist_ok=True)
            except OSError as e:
                logging.info(f"create_directory OSError::: {e}")
                raise AirflowSkipException()
            logging.info(f"create_directory full_file_path::: {full_file_path}")
            return file_path
        
        @task
        def call_url(collect_data_list,file_path,**kwargs):
            """
            조건별 URL 설정 및 호출하여 dw 적재
            params: tdm_list_url_info, tdm_file_url_info, tdm_standard_url_info, th_data_clct_mastr_log, tn_clct_file_info, file_path
            return: file_size
            """
            import requests
            import os
            import time
            from util.call_url_util import CallUrlUtil
            from xml_to_dict import XMLtoDict

            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            # log_full_file_path = collect_data_list['log_full_file_path']
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path

            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
            link_se_cd = tn_data_bsc_info.link_se_cd.lower()
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd.lower()
            pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd.lower()
            base_url = return_url = tn_data_bsc_info.link_data_clct_url

            # 파라미터 및 파라미터 길이 설정
            data_interval_start = now()  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
            data_interval_end = now()  # 실제 실행하는 날짜를 KST 로 설정
            params_dict, params_len = CallUrlUtil.set_params(tn_data_bsc_info, session, data_interval_start, data_interval_end, kwargs)

            retry_num = 0  # 데이터 없을 시 재시도 횟수
            repeat_num = 1  # 파라미터 길이만큼 반복 호출 횟수
            page_no = 1  # 현재 페이지
            total_page = 1  # 총 페이지 수
            
            header = True   # 파일 헤더 모드
            mode = "w"  # 파일 쓰기 모드 overwrite

            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
            source_file_name =  tn_clct_file_info.insd_file_nm + "." + tn_data_bsc_info.pvdr_sou_data_pvsn_stle  # 원천 파일명
            full_file_path = root_collect_file_path + file_path
            full_file_name = full_file_path + file_name
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr
            file_size = 0  # 파일 사이즈
            row_count = 0  # 행 개수

            # 카운터 초기화 (file-data-list 타입에서만 사용)
            null_atchfile_count = [0]
            valid_atchfile_count = [0]
            api_error_count = [0]

            try:
                # 파라미터 길이만큼 반복 호출
                while repeat_num <= params_len:
                    
                    # 총 페이지 수만큼 반복 호출
                    while page_no <= total_page:
                        
                        # 파라미터 길이만큼 호출 시 while 종료
                        if repeat_num > params_len:
                            break
                    
                        # 재시도 5회 이상 시
                        if retry_num >= 5:
                            # 파라미터 길이 == 1) whlie 종료
                            if params_len == 1:
                                repeat_num += 1
                                break
                            else:  # 파라미터 길이 != 1)
                                # th_data_clct_contact_fail_hstry_log 에 입력
                                CallUrlUtil.insert_fail_history_log(th_data_clct_mastr_log, return_url, file_path, session, params_dict['param_list'][repeat_num - 1], page_no)
                                # 총 페이지 수만큼 덜 돌았을 때
                                if page_no < total_page:  # 다음 페이지 호출
                                    retry_num = 0
                                    page_no += 1
                                    continue
                                # 총 페이지 수만큼 다 돌고
                                elif page_no == total_page:
                                    # 파라미터 길이만큼 덜 돌았을 때
                                    if repeat_num < params_len:
                                        retry_num = 0
                                        page_no = 1
                                        repeat_num += 1
                                        continue
                                    # 파라미터 길이만큼 다 돌았을 때
                                    else:
                                        repeat_num += 1
                                        break
                        # url 설정
                        return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, link_se_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"

                        # url 호출
                        response = requests.get(return_url, verify= False)
                        response_code = response.status_code        

                        # url 호출 시 메세지 설정
                        header, mode = CallUrlUtil.get_request_message(retry_num, repeat_num, page_no, return_url, total_page, full_file_name, header, mode)

                        if response_code == 200:
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "json" and 'OpenAPI_ServiceResponse' not in response.text:  # 공공데이터포털 - HTTP 에러 제외
                                json_data = response.json()
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "xml" or 'OpenAPI_ServiceResponse' in response.text:  # 공공데이터포털 - HTTP 에러 시 xml 형태
                                json_data = XMLtoDict().parse(response.text)

                            # 원천 데이터 저장
                            CallUrlUtil.create_source_file(json_data, source_file_name, full_file_path, mode)

                            # 공공데이터포털 - HTTP 에러 시
                            if 'OpenAPI_ServiceResponse' in response.text:
                                retry_num += 1
                                continue

                            result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                            result_json = result['result_json_array']
                            result_size = len(result_json)

                            # 데이터 구분값 확인
                            data_se_val_two = tn_data_bsc_info.pvdr_data_se_vl_two.lower() if tn_data_bsc_info.pvdr_data_se_vl_two else ''

                            # file-data-list 타입인 경우 추가 API 호출 및 파일 다운로드
                            if data_se_val_two == "file-data-list" or data_se_val_two == "standard-data-list":
                                # 각 항목에 대해 추가 API 호출
                                for dict_value in result_json:
                                    null_atchfile_count, valid_atchfile_count, api_error_count = call_additional_api_and_save_csv(
                                        dict_value, full_file_path, null_atchfile_count, valid_atchfile_count, api_error_count
                                    )
                                
                                logging.info(f"📊 페이지 {page_no} 추가 파일 생성 통계 :: "
                                            f"API 응답 건수: {len(result_json)}, "
                                            f"실제 처리 시도: {valid_atchfile_count[0] + null_atchfile_count[0] + api_error_count[0]}, "
                                            f"정상 생성: {valid_atchfile_count[0]}, "
                                            f"생략: {null_atchfile_count[0]}, "
                                            f"호출실패: {api_error_count[0]}")
                                # logging.info(f"📊 페이지 {page_no} 추가 파일 생성 통계 :: 총 대상 건수: {valid_atchfile_count[0] + null_atchfile_count[0]+ api_error_count[0]}, "
                                #             f"정상 생성: {valid_atchfile_count[0]}, 생략: {null_atchfile_count[0]}, 호출실패: {api_error_count[0]}")

                            # 일반적인 CSV 저장 로직 (모든 타입에 대해 실행)
                            if result_size != 0:
                                retry_num = 0  # 재시도 횟수 초기화
                                if page_no == 1: # 첫 페이지일 때
                                    # 페이징 계산
                                    total_count = int(result['total_count'])
                                    total_page = CallUrlUtil.get_total_page(total_count, result_size)

                                row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                                if row_count == 0:
                                    header = True
                                    mode = "w"

                                # csv 파일 생성
                                CallUrlUtil.create_csv_file(link_file_sprtr, th_data_clct_mastr_log.data_crtr_pnttm, th_data_clct_mastr_log.clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no)

                                row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                                if row_count != 0:
                                    logging.info(f"현재까지 파일 내 행 개수: {row_count}")
                                
                                # 페이지 증가
                                page_no += 1

                                # 총 페이지 수 == 1)
                                if total_page == 1:
                                    repeat_num += 1
                                    break
                                else:
                                    if page_no <= total_page:
                                        continue
                                    elif page_no > total_page:
                                        if params_len == 1:
                                            repeat_num += 1
                                            break
                                        elif params_len != 1:
                                            if repeat_num < params_len:
                                                page_no = 1
                                                repeat_num += 1
                                            else: 
                                                repeat_num += 1
                                                break
                            else:
                                logging.info(f"call_url_process resultmsg::: NO_DATA")
                                retry_num += 1
                                continue
                        else:
                            logging.info(f"call_url_process response_code::: {response_code}")
                            retry_num += 1
                            continue

                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    file_size = os.path.getsize(full_file_name)
                logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")
                logging.info(f"call_url::: 수집 끝")

            except AirflowSkipException as e:
                raise e
            except Exception as e:
                logging.info(f"call_url Exception::: {e}")
                raise e

        file_path = create_directory(collect_data_list)
        file_path >> call_url(collect_data_list, file_path) 
                
    collect_data_list = collect_data_info()
    call_url_process.expand(collect_data_list = collect_data_list)

dag_object = api_dw_month_data_1st()

# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2025,7,28,9,00),
        conn_file_path=conn_path,
    )