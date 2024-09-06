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
from util.call_url_util import CallUrlUtil
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST

from util.date_custom_util import DateUtil
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.tdm_list_url_info import TdmListUrlInfo
from dto.tdm_file_url_info import TdmFileUrlInfo
from dto.tdm_standard_url_info import TdmStandardUrlInfo
from airflow.exceptions import AirflowSkipException

@dag(
    dag_id="sdag_api_dw_month_data_1st",
    schedule="@monthly",
    start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
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
                            order by sn
                            ;
                            '''
        data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
        data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")   # 실제 실행하는 날짜를 KST 로 설정
        # collect_data_list = []
        collect_data_list = CommonUtil.insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs)
        if collect_data_list == []:
            logging.info(f"select_collect_data_fail_info ::: 수집 대상없음 프로그램 종료")
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
            root_collect_file_path = kwargs['var']['value'].root_collect_file_path
            file_path = CommonUtil.create_directory(collect_data_list, session, data_interval_end, root_collect_file_path, "n")
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
            # tdm_list_url_info = TdmListUrlInfo(**collect_data_list['tdm_list_url_info'])
            # tdm_file_url_info = TdmFileUrlInfo(**collect_data_list['tdm_file_url_info'])
            # tdm_standard_url_info = TdmStandardUrlInfo(**collect_data_list['tdm_standard_url_info'])
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']

            dtst_cd = th_data_clct_mastr_log.dtst_cd.lower()
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
            # print("result !!!! ", {len(collect_data_list.dict)})

            # 데이터셋 코드별 파일 이름
            if dtst_cd == 'data919':
                table_name = TdmListUrlInfo.__tablename__
            elif dtst_cd == 'data920':
                table_name = TdmFileUrlInfo.__tablename__
            else :
                table_name = TdmStandardUrlInfo.__tablename__
            # table_name = TdmListUrlInfo.__tablename__
            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부
            file_name = tn_clct_file_info.insd_file_nm + "." + tn_clct_file_info.insd_file_extn  # csv 파일명
            # file_name = tn_data_bsc_info_test.dtst_nm + ".csv"  # csv 파일명
            source_file_name =  tn_clct_file_info.insd_file_nm + "." + tn_data_bsc_info.pvdr_sou_data_pvsn_stle  # 원천 파일명
            full_file_path = root_collect_file_path + file_path
            full_file_name = full_file_path + file_name
            link_file_sprtr = tn_data_bsc_info.link_file_sprtr
            file_size = 0  # 파일 사이즈
            row_count = 0  # 행 개수

            try:
                # 테이블 적재 전에 TRUNCATE 시키기
                # with session.begin() as conn:
                #     delete_stmt = f'TRUNCATE TABLE {table_name};'
                #     conn.execute(delete_stmt)

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
                        return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"
                        # return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no)}"

                        # url 호출
                        response = requests.get(return_url, verify= False)
                        response_code = response.status_code        

                        # url 호출 시 메세지 설정
                        header, mode = CallUrlUtil.get_request_message(retry_num, repeat_num, page_no, return_url, total_page, full_file_name, header, mode)

                        # response = requests.get(base_url, verify=False)
                        # if response_code == 200 and 'NO_DATA' not in response.text:
                        if response_code == 200:
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "json" and 'OpenAPI_ServiceResponse' not in response.text and '제공 가능한 데이터가 없습니다' not in response.text:  # 공공데이터포털 - HTTP 에러 제외
                                json_data = response.json()
                            if tn_data_bsc_info.pvdr_sou_data_pvsn_stle == "xml" or 'OpenAPI_ServiceResponse' in response.text or '제공 가능한 데이터가 없습니다' in response.text:  # 공공데이터포털, 지역별 독서량_독서율(20240411 이후 변경) - HTTP 에러 시 xml 형태
                                json_data = XMLtoDict().parse(response.text)
                            # print("response_code!!!!!!", response_code)
                            # json_data = response.json()
                            
                            #원천 파일 저장
                            # json_file_name = os.path.join(full_file_path, f"{source_file_name}.json")
                            # with open(json_file_name, 'w', encoding='utf-8') as f:
                            #     f.write(response.text)

                            CallUrlUtil.create_source_file(json_data, source_file_name, full_file_path, mode)

                            # 공공데이터포털 - HTTP 에러 시
                            if 'OpenAPI_ServiceResponse' in response.text:
                                retry_num += 1
                                continue

                            # json 읽어오기
                            # result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                            # result = CallUrlUtil.read_json(json_data, dtst_cd)
                            result = CallUrlUtil.read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, tn_data_bsc_info.data_se_col_one)
                            result_json = result['result_json_array']
                            result_size = len(result_json)

                            # 데이터 존재 시
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

                                data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
                                link_file_sprtr = th_data_clct_mastr_log.link_file_sprtr
                                
                                # delete_table(tn_data_bsc_info, data_crtr_pnttm)

                                # DW 적재시 clct_sn 증가값
                                clct_sn_counter = 1
                                
                                # 데이터셋 코드별 데이터 추출
                                extracted_data = []
                                for item in json_data.get('data', []):
                                    if dtst_cd == 'data919':
                                        extracted_data.append({
                                            'clct_sn': clct_sn_counter , 
                                            'category_cd': item.get('category_cd'), 
                                            'category_nm': item.get('category_nm'), 
                                            'collection_method': item.get('collection_method'), 
                                            'created_at': item.get('created_at'), 
                                            '"desc"': item.get('desc'), 
                                            'download_cnt': item.get('download_cnt'), 
                                            'ext': item.get('ext'), 
                                            'id': item.get('id'), 
                                            'is_deleted': item.get('is_deleted'), 
                                            'is_requested_data': item.get('is_requested_data'), 
                                            'keywords': item.get('keywords'), 
                                            'list_type': item.get('list_type'), 
                                            'new_category_cd': item.get('new_category_cd'), 
                                            'new_category_nm': item.get('new_category_nm'), 
                                            'org_cd': item.get('org_cd'), 
                                            'org_nm': item.get('org_nm'), 
                                            'ownership_grounds': item.get('ownership_grounds'), 
                                            'page_url': item.get('page_url'), 
                                            'providing_scope': item.get('providing_scope'), 
                                            'register_status': item.get('register_status'), 
                                            'title': item.get('title'), 
                                            'updated_at': item.get('updated_at'), 
                                            'view_cnt': item.get('view_cnt'), 
                                            'data_crtr_pnttm': th_data_clct_mastr_log.data_crtr_pnttm,
                                            'clct_pnttm': DateUtil.get_ymdhm(),
                                            'clct_log_sn': th_data_clct_mastr_log.clct_log_sn,
                                            'page_no': page_no
                                            # 필요한 모든 필드를 추가
                                        })
                                        clct_sn_counter += 1

                                    elif dtst_cd == 'data920':
                                        extracted_data.append({
                                            'clct_sn': clct_sn_counter , 
                                            'core_data_nm': item.get('core_data_nm'),
                                            'cost_unit': item.get('cost_unit'),
                                            'created_at': item.get('created_at'),
                                            'data_limit': item.get('data_limit'),
                                            'data_type': item.get('data_type'),
                                            'dept_nm': item.get('dept_nm'),
                                            '"desc"': item.get('desc'),
                                            'download_cnt': item.get('download_cnt'),
                                            'etc': item.get('etc'),
                                            'ext': item.get('ext'),
                                            'id': item.get('id'),
                                            'is_charged': item.get('is_charged'),
                                            'is_copyrighted': item.get('is_copyrighted'),
                                            'is_core_data': item.get('is_core_data'),
                                            'is_deleted': item.get('is_deleted'),
                                            'is_list_deleted': item.get('is_list_deleted'),
                                            'is_std_data': item.get('is_std_data'),
                                            'is_third_party_copyrighted': item.get('is_third_party_copyrighted'),
                                            'keywords': item.get('keywords'),
                                            'list_id': item.get('list_id'),
                                            'list_title': item.get('list_title'),
                                            'media_cnt': item.get('media_cnt'),
                                            'media_type': item.get('media_type'),
                                            'meta_url': item.get('meta_url'),
                                            'new_category_cd': item.get('new_category_cd'),
                                            'new_category_nm': item.get('new_category_nm'),
                                            'next_registration_date': item.get('next_registration_date'),
                                            'org_cd': item.get('org_cd'),
                                            'org_nm': item.get('org_nm'),
                                            'ownership_grounds': item.get('ownership_grounds'),
                                            'regist_type': item.get('regist_type'),
                                            'register_status': item.get('register_status'),
                                            'share_scope_nm': item.get('share_scope_nm'),
                                            'title': item.get('title'),
                                            'update_cycle': item.get('update_cycle'),
                                            'updated_at': item.get('updated_at'),
                                            'data_crtr_pnttm': th_data_clct_mastr_log.data_crtr_pnttm,
                                            'clct_pnttm': DateUtil.get_ymdhm(),
                                            'clct_log_sn': th_data_clct_mastr_log.clct_log_sn,
                                            'page_no': page_no
                                            # 필요한 모든 필드를 추가
                                        })
                                        clct_sn_counter += 1
                                    else:
                                        extracted_data.append({
                                            'clct_sn': clct_sn_counter ,
                                            'category_cd': item.get('category_cd'),
                                            'category_nm': item.get('category_nm'),
                                            'collection_method': item.get('collection_method'),
                                            'created_at': item.get('created_at'),
                                            'dept_nm': item.get('dept_nm'),
                                            '"desc"': item.get('desc'),
                                            'id': item.get('id'),
                                            'is_requested_data': item.get('is_requested_data'),
                                            'keywords': item.get('keywords'),
                                            'list_id': item.get('list_id'),
                                            'list_title': item.get('list_title'),
                                            'list_type': item.get('list_type'),
                                            'new_category_cd': item.get('new_category_cd'),
                                            'new_category_nm': item.get('new_category_nm'),
                                            'next_registration_date': item.get('next_registration_date'),
                                            'org_cd': item.get('org_cd'),
                                            'org_nm': item.get('org_nm'),
                                            'ownership_grounds': item.get('ownership_grounds'),
                                            'providing_scope': item.get('providing_scope'),
                                            'req_cnt': item.get('req_cnt'),
                                            'title': item.get('title'),
                                            'updated_dt': item.get('updated_dt'),
                                            'updated_dt': item.get('updated_dt'),
                                            'data_crtr_pnttm': th_data_clct_mastr_log.data_crtr_pnttm,
                                            'clct_pnttm': DateUtil.get_ymdhm(),
                                            'clct_log_sn': th_data_clct_mastr_log.clct_log_sn,
                                            'page_no': page_no
                                        })
                                        clct_sn_counter += 1

                                with session.begin() as conn:
                                    for row in extracted_data:
                                        columns = ', '.join(row.keys())
                                        # values = ', '.join([f"'{v}'" for v in row.values()])
                                        values = ', '.join([f"'{v}'" if v is not None else 'NULL' for v in row.values()])
                                        insert_stmt = f'''
                                            INSERT INTO {table_name} ({columns}) VALUES ({values});
                                        '''
                                        conn.execute(insert_stmt)

                                # row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                                # if row_count == 0:
                                #     header = True
                                #     mode = "w"

                                # # csv 파일 생성
                                # CallUrlUtil.create_csv_file(link_file_sprtr, th_data_clct_mastr_log.data_crtr_pnttm, th_data_clct_mastr_log.clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no)
                        
                            # row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
                            # if row_count != 0:
                            #     logging.info(f"현재까지 파일 내 행 개수: {row_count}")
                            # page_no += 1


                            # 총 페이지 수 == 1)
                            if total_page == 1:
                                repeat_num += 1
                                break
                            else:
                                if page_no < total_page:
                                    page_no += 1
                                elif page_no == total_page:
                                    if params_len == 1:
                                        repeat_num += 1
                                        break
                                    elif params_len != 1:
                                        if repeat_num < params_len:
                                            page_no = 1
                                            repeat_num += 1
                                        else: repeat_num += 1
                                        break
                        else:
                            logging.info(f"call_url_process resultmsg::: NO_DATA")
                            retry_num += 1
                            continue
                # 파일 사이즈 확인
                if os.path.exists(full_file_name):
                    file_size = os.path.getsize(full_file_name)
                logging.info(f"call_url file_name::: {file_name}, file_size::: {file_size}")

                # 실패 로그 개수 확인
                fail_count = CallUrlUtil.get_fail_data_count(th_data_clct_mastr_log.clct_log_sn, session)
                
                if row_count == 0 and fail_count == 0 and retry_num < 5:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP_NO_DATA, "n")
                    raise AirflowSkipException()
                elif fail_count != 0 or retry_num >= 5:
                    logging.info(f"call_url ::: {CONST.MSG_CLCT_ERROR_CALL}")
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
                    raise AirflowSkipException()
                else:
                    # tn_clct_file_info 수집파일정보
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, tn_clct_file_info.insd_file_nm, file_path, tn_data_bsc_info.link_file_extn, file_size, None)
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_COMP, CONST.MSG_CLCT_COMP, "n")
                    if link_file_crt_yn == "y":
                        CommonUtil.update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, tn_clct_file_info.insd_file_nm, file_path, tn_clct_file_info.insd_file_extn, file_size)
            except AirflowSkipException as e:
                raise e
            except Exception as e:
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_CALL, "n")
                logging.info(f"call_url Exception::: {e}")
                raise e


        @task(trigger_rule='all_done')
        def insert_data_info(collect_data_list,**kwargs):
            """
            DW 적재 (tn_data_bsc_info에 필요한 데이터만 각각 추출하여 적재)
            params : collect_data_list, tdm_list_url_info, tdm_file_url_info, tdm_standard_url_info
            """
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
            dtst_cd = tn_data_bsc_info.dtst_cd

            try:
                with session.begin() as conn:
                    
                    if dtst_cd in ['data919']:
                        # 목록과 파일 데이터 Join 후 bsc에 insert하는 함수
                        query = f"SELECT fn_data_file_data_list_updt('{data_crtr_pnttm}');"
                        conn.execute(query)
                        logging.info(f"Query executed: {query}")
                        logging.info(f"fn_data_file_data_list_updt completed successfully. Inserted {query} rows.")
                    
                    if dtst_cd == 'data922':
                        # 표준 데이터 bsc에 insert하는 함수
                        query = f"SELECT fn_data_std_data_list_updt('{data_crtr_pnttm}');"
                        conn.execute(query)
                        logging.info(f"Query executed: {query}")
                        logging.info(f"fn_data_std_data_list_updt completed successfully. Inserted {query} rows.")
            
            except Exception as e:
                logging.error(f"insert_data_info Exception for data_crtr_pnttm {data_crtr_pnttm}::: {e}")
                raise e


        @task
        def check_loading_result(collect_data_list):
            """
            DW 적재 결과 확인
            params: collect_data_list
            """
            # tdm_list_url_info = TdmListUrlInfo(**collect_data_list['tdm_list_url_info'])
            # dw_tbl_phys_nm = TdmListUrlInfo.dw_tbl_phys_nm
            # tn_data_bsc_info_test = TnDataBscInfo(**collect_data_list['tn_data_bsc_info_test'])
            tn_data_bsc_info = TnDataBscInfo(**collect_data_list['tn_data_bsc_info'])
            dw_tbl_phys_nm = tn_data_bsc_info.dw_tbl_phys_nm
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_list['th_data_clct_mastr_log'])
            data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
            
            tn_clct_file_info = TnClctFileInfo(**collect_data_list['tn_clct_file_info'])
            log_full_file_path = collect_data_list['log_full_file_path']
            # dw_tbl_phys_nm = TnDataBscInfo.__tablename__

            result_count = 0
            get_count_stmt = f"""SELECT COUNT(data_crtr_pnttm) FROM {dw_tbl_phys_nm} WHERE data_crtr_pnttm = '{data_crtr_pnttm}'"""
            try:
                with session.begin() as conn:
                    result_count = conn.execute(get_count_stmt).first()[0]
                    th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, collect_data_list['th_data_clct_mastr_log']['clct_log_sn'])
                    th_data_clct_mastr_log.dw_rcrd_cnt = result_count
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_DW_LDADNG, CONST.STTS_COMP, CONST.MSG_DW_LDADNG_COMP, "n")
                    logging.info(f"check_loading_result dw_rcrd_cnt::: {result_count}")
            except Exception as e:
                logging.error(f"check_loading_result Exception::: {e}")
                raise e

        file_path = create_directory(collect_data_list)
        file_path >> call_url(collect_data_list, file_path) >> insert_data_info(collect_data_list) >> check_loading_result(collect_data_list)
                
    collect_data_list = collect_data_info()
    call_url_process.expand(collect_data_list = collect_data_list)
    # collect_data_list = collect_data_info()

dag_object = api_dw_month_data_1st()


# only run if the module is the main program
if __name__ == "__main__":
    conn_path = "../connections_minio_pg.yaml"
    # variables_path = "../variables.yaml"
    dtst_cd = ""

    dag_object.test(
        execution_date=datetime(2024,8,22,15,00),
        conn_file_path=conn_path,
        # variable_file_path=variables_path,
        # run_conf={"dtst_cd": dtst_cd},
    )