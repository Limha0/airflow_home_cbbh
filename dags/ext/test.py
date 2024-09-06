# import logging
# import os
# import urllib3
# import sys

# from pendulum import datetime, from_format,now
# from airflow.decorators import dag, task, task_group
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from sqlalchemy.orm import sessionmaker
# from util.file_util import FileUtil
# from util.common_util import CommonUtil
# from util.call_url_util import CallUrlUtil

# from dto.tn_data_bsc_info_test import TnDataBscInfo
# from dto.tdm_list_url_info import TdmListUrlInfo
# from dto.tdm_file_url_info import TdmFileUrlInfo
# from airflow.exceptions import AirflowSkipException
# # from util.common_util import CommonUtil

# @dag(
#     dag_id="sdag_api_dw_test",
#     schedule="@monthly",
#     start_date=datetime(2023, 9, 16, tz="Asia/Seoul"),  # UI 에 KST 시간으로 표출하기 위한 tz 설정
#     catchup=False,
#     # render Jinja template as native Python object
#     render_template_as_native_obj=True,
#     tags=["test"],
# )
# def insert_tdm_list_url():

#     # PostgresHook 객체 생성
#     pg_hook = PostgresHook(postgres_conn_id='local_db_conn')

#     #sqlalchemy 를 이용한 connection
#     engine = pg_hook.get_sqlalchemy_engine()

#     # sqlalchey session 생성
#     session = sessionmaker(engine, expire_on_commit=False)

#     @task
#     def collect_data_info(**kwargs): # 수집 데이터 정보 조회
#         """
#         tn_data_bsc_info_test 테이블에서 수집 대상 기본 정보 조회 후 tdm_list_url_info 테이블에 입력
#         return: collect_data_list
#         """
#         print("hello")
#         # print("kwargs :" , kwargs)
#         select_bsc_info_stmt = '''
#                             select *
#                             from tn_data_bsc_info_test
#                             where 1=1
#                             and link_se_cd = 'fixed'
#                             '''
#         data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
#         data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")   # 실제 실행하는 날짜를 KST 로 설정
#         collect_data_list = []
#         # collect_data_list = CommonUtil.insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs)
#         with session.begin() as conn:
#             # execute : 쿼리를 실행하는 메서드
#             # .all : 쿼리 실행 결과를 가져오는 메서드 ( 모든 결과 행을 리스트로 반환)
#             # 각 행을 실행하여 리스트를 dict_row에 값을 담는다
#             for dict_row in conn.execute(select_bsc_info_stmt).all():
#                 print("result !!!! ", dict_row)
#                 tn_data_bsc_info_test = TnDataBscInfo(**dict_row)
#                 collect_data_list.append({
#                                 # 객체를 파이썬에서 사용할 수 있는 데이터 형태로 변환
#                                 "tn_data_bsc_info_test" : tn_data_bsc_info_test.as_dict()
#                                 })
#         print("hello2")
#         return collect_data_list

#     # insert_collect_data_info() 

#     @task_group(group_id='call_url_process')
#     def call_url_process(collect_data_list):
#         # from util.file_util import FileUtil

#         @task
#         def create_directory(collect_data_list, **kwargs):
#             """
#             수집 파일 경로 생성
#             params: tn_data_bsc_info, th_data_clct_mastr_log, tn_clct_file_info
#             return: file_path: tn_clct_file_info 테이블에 저장할 파일 경로
#             """
#             data_interval_end = now()  # 실제 실행하는 날짜를 KST 로 설정
#             root_collect_file_path = kwargs['var']['value'].root_collect_file_path
#             # root_collect_file_path = f"C:/airflow_test/{data_interval_end.format('YYYYMMDD')}/"
#             # root_collect_file_path = f"/home/limhayoung/data/collect/"
#             file_path = CommonUtil.create_directory(collect_data_list, session, data_interval_end, root_collect_file_path, "n")
#             # os.makedirs(root_collect_file_path, exist_ok=True)
#             return file_path

#         @task
#         def call_url(collect_data_list,file_path, **kwargs):
#             import requests
#             print("hello")


#             tn_data_bsc_info_test = TnDataBscInfo(**collect_data_list['tn_data_bsc_info_test'])

#             dtst_cd = tn_data_bsc_info_test.dtst_cd.lower()
#             root_collect_file_path = kwargs['var']['value'].root_collect_file_path
#             base_url = return_url = tn_data_bsc_info_test.list_url

#             # tdm_list_url_info = TdmListUrlInfo(**collect_data_list['tdm_list_url_info'])
#             data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")  # 처리 데이터의 시작 날짜 (데이터 기준 시점)
#             data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")
#             params_dict, params_len = CallUrlUtil.set_params(tn_data_bsc_info_test, session, data_interval_start, data_interval_end, kwargs)

#             retry_num = 0  # 데이터 없을 시 재시도 횟수
#             repeat_num = 1  # 파라미터 길이만큼 반복 호출 횟수
#             page_index = 1  # 현재 페이지
#             total_page = 1  # 총 페이지 수

#             header = False
#             mode = "w"  # 파일 쓰기 모드 overwrite 
#             table_name = TdmListUrlInfo.__tablename__
#             link_file_crt_yn = tn_data_bsc_info_test.link_file_crt_yn.lower()  # csv 파일 생성 여부
#             # file_name = tn_data_bsc_info_test.insd_file_nm + "." + tn_data_bsc_info_test.insd_file_extn  # csv 파일명
#             file_name = "공공데이터포털_목록API_URL" + "." + 'csv'  # csv 파일명
#             # source_file_name = tn_data_bsc_info_test.insd_file_nm + "." + tn_data_bsc_info_test.pvdr_sou_data_pvsn_stle  # 원천 파일명
#             source_file_name = "공공데이터포털_목록API_URL" + "." + 'json'  # 원천 파일명
#             full_file_path = root_collect_file_path + file_path
#             full_file_name = full_file_path + file_name
#             # link_file_sprtr = tn_data_bsc_info_test.link_file_sprtr
#             link_file_sprtr = ','
#             file_size = 0  # 파일 사이즈
#             row_count = 0  # 행 개수

#             try:
#                 # 테이블 적재 전에 TRUNCATE 시키기
#                 with session.begin() as conn:
#                     delete_stmt = f'TRUNCATE TABLE {table_name};'
#                     conn.execute(delete_stmt)

#                 # 파라미터 길이만큼 반복 호출
#                 while repeat_num <= params_len:
                    
#                     # 총 페이지 수만큼 반복 호출
#                     while page_index <= total_page:
                        
                        
#                         # 파라미터 길이만큼 호출 시 while 종료
#                         if repeat_num > params_len:
#                             break
                    
#                         # 재시도 5회 이상 시
#                         if retry_num >= 5:
#                             # 파라미터 길이 == 1) whlie 종료
#                             if params_len == 1:
#                                 repeat_num += 1
#                                 break
#                             else:  # 파라미터 길이 != 1)
                            
#                                 # 총 페이지 수만큼 덜 돌았을 때
#                                 if page_index < total_page:  # 다음 페이지 호출
#                                     retry_num = 0
#                                     page_index += 1
#                                     continue
#                                 # 총 페이지 수만큼 다 돌고
#                                 elif page_index == total_page:
#                                     # 파라미터 길이만큼 덜 돌았을 때
#                                     if repeat_num < params_len:
#                                         retry_num = 0
#                                         page_index = 1
#                                         repeat_num += 1
#                                         continue
#                                     # 파라미터 길이만큼 다 돌았을 때
#                                     else:
#                                         repeat_num += 1
#                                         break

#                         # url 설정
#                         return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, params_dict, repeat_num, page_index)}"  

#                         # url 호출
#                         response = requests.get(return_url, verify= False)
#                         response_code = response.status_code

#                         # url 호출 시 메세지 설정
#                         header, mode = CallUrlUtil.get_request_message(retry_num, repeat_num, page_index, return_url, total_page, full_file_name, header, mode)

#                         if response_code == 200 and 'NO_DATA' not in response.text:
#                             print("response_code!!!!!!", response_code)
#                             json_data = response.json()
                            
#                             #원천 파일 저장
#                             json_file_name = os.path.join(full_file_path, f"{dtst_cd}_{repeat_num}_{page_index}.json")
#                             with open(json_file_name, 'w', encoding='utf-8') as f:
#                                 f.write(response.text)

#                             # json 읽어오기
#                             result = CallUrlUtil.read_json(json_data, dtst_cd)
#                             result_json = result['result_json_array']
#                             result_size = len(result_json)

#                             # 데이터 존재 시
#                             if result_size != 0:
#                                 retry_num = 0  # 재시도 횟수 초기화
#                                 if page_index == 1: # 첫 페이지일 때
#                                     # 페이징 계산
#                                     total_count = int(result['total_count'])
#                                     total_page = CallUrlUtil.get_total_page(total_count, result_size)

#                                 # 데이터베이스에 삽입할 데이터 추출
#                                 extracted_data = []
#                                 for item in json_data.get('data', []):
#                                     extracted_data.append({
#                                         'category_cd': item.get('category_cd'),
#                                         'category_nm': item.get('category_nm'),
#                                         'collection_method': item.get('collection_method'),
#                                         'created_at': item.get('created_at'),
#                                         '"desc"': item.get('desc'),
#                                         'download_cnt': item.get('download_cnt'),
#                                         'ext': item.get('ext'),
#                                         'id': item.get('id'),
#                                         'is_deleted': item.get('is_deleted'),
#                                         'is_requested_data': item.get('is_requested_data'),
#                                         'keywords': item.get('keywords'),
#                                         'list_type': item.get('list_type'),
#                                         'new_category_cd': item.get('new_category_cd'),
#                                         'new_category_nm': item.get('new_category_nm'),
#                                         'org_cd': item.get('org_cd'),
#                                         'org_nm': item.get('org_nm'),
#                                         'ownership_grounds': item.get('ownership_grounds'),
#                                         'page_url': item.get('page_url'),
#                                         'providing_scope': item.get('providing_scope'),
#                                         'register_status': item.get('register_status'),
#                                         'title': item.get('title'),
#                                         'updated_at': item.get('updated_at'),
#                                         'view_cnt': item.get('view_cnt')
#                                         # 필요한 모든 필드를 추가
#                                     })

#                                 # 데이터베이스에 데이터 삽입
#                                 with session.begin() as conn:
#                                     # table_name = tdm_list_url_info.dw_tbl_phys_nm
#                                     for row in extracted_data:
#                                         columns = ', '.join(row.keys())
#                                         values = ', '.join([f"'{v}'" for v in row.values()])
#                                         # delete_stmt = f'''
#                                         # TRUNCATE TABLE {table_name};
#                                         # '''
#                                         # conn.execute(delete_stmt)
#                                         insert_stmt = f'''
#                                             INSERT INTO {table_name} ({columns}) VALUES ({values});
#                                         '''
#                                         conn.execute(insert_stmt)
                            
#                             # 데이터 존재 시
#                             # if result_size != 0:
#                             #     retry_num = 0  # 재시도 횟수 초기화
#                             #     if page_index == 1: # 첫 페이지일 때
#                             #         # 페이징 계산
#                             #         total_count = int(result['total_count'])
#                             #         total_page = CallUrlUtil.get_total_page(total_count, result_size)

#                                 row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
#                                 if row_count == 0:
#                                     header = True
#                                     mode = "w"

#                                 # csv 파일 생성
#                                 CallUrlUtil.create_csv_file(link_file_sprtr, '202407', '1', full_file_path, file_name, result_json, header, mode, page_index)

#                             row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
#                             if row_count != 0:
#                                 logging.info(f"현재까지 파일 내 행 개수: {row_count}")
#                             # page_index += 1
#                             # 총 페이지 수 == 1)
#                             if total_page == 1:
#                                 repeat_num += 1
#                                 break
#                             else:
#                                 if page_index < total_page:
#                                     page_index += 1
#                                 elif page_index == total_page:
#                                     if params_len == 1:
#                                         repeat_num += 1
#                                         break
#                                     elif params_len != 1:
#                                         if repeat_num < params_len:
#                                             page_index = 1
#                                             repeat_num += 1
#                                         else: repeat_num += 1
#                                         break
#                         else:
#                             logging.info(f"call_url resultmsg::: NO_DATA")
#                             retry_num += 1
#                             continue
#             except AirflowSkipException as e:
#                 raise e
#             except Exception as e:
#                 logging.info(f"call_url Exception::: {e}")
#                 raise e
            
#         @task
#         def file_url_call(collect_data_list,**kwargs):
#             print("hello_file!!!!!!!!")
#             import requests
#             tn_data_bsc_info_test = TnDataBscInfo(**collect_data_list['tn_data_bsc_info_test'])
#             dtst_cd = tn_data_bsc_info_test.dtst_cd.lower()
#             base_url = tn_data_bsc_info_test.file_url
#             root_collect_file_path = kwargs['var']['value'].root_collect_file_path

#             data_interval_start = kwargs['data_interval_start'].in_timezone("Asia/Seoul")
#             data_interval_end = kwargs['data_interval_end'].in_timezone("Asia/Seoul")
#             params_dict, params_len = CallUrlUtil.set_params(tn_data_bsc_info_test, session, data_interval_start, data_interval_end, kwargs)

#             retry_num = 0
#             repeat_num = 1
#             page_index = 1
#             total_page = 1

#             header = False
#             mode = "w"
#             table_name = TdmFileUrlInfo.__tablename__
#             file_name = "공공데이터포털_파일API_URL" + "." + 'csv'  # csv 파일명
#             # source_file_name = tn_data_bsc_info_test.insd_file_nm + "." + tn_data_bsc_info_test.pvdr_sou_data_pvsn_stle  # 원천 파일명
#             source_file_name = "공공데이터포털_파일API_URL"  # 원천 파일명
#             full_file_path = root_collect_file_path + file_path
#             full_file_name = full_file_path + file_name
#             # link_file_sprtr = tn_data_bsc_info_test.link_file_sprtr
#             link_file_sprtr = ','
#             file_size = 0  # 파일 사이즈
#             row_count = 0  # 행 개수

#             meta_urls = [] # meta_url 담을 빈 리스트 생성

#             try:
#                 with session.begin() as conn:
#                     delete_stmt = f'TRUNCATE TABLE {table_name};'
#                     conn.execute(delete_stmt)

#                 while repeat_num <= params_len:
#                     while page_index <= total_page:
#                         if repeat_num > params_len:
#                             break

#                         if retry_num >= 5:
#                             if params_len == 1:
#                                 repeat_num += 1
#                                 break
#                             else:
#                                 if page_index < total_page:
#                                     retry_num = 0
#                                     page_index += 1
#                                     continue
#                                 elif page_index == total_page:
#                                     if repeat_num < params_len:
#                                         retry_num = 0
#                                         page_index = 1
#                                         repeat_num += 1
#                                         continue
#                                     else:
#                                         repeat_num += 1
#                                         break

#                         return_url = f"{base_url}{CallUrlUtil.set_url(dtst_cd, params_dict, repeat_num, page_index)}"
#                         response = requests.get(return_url, verify=False)
#                         response_code = response.status_code

#                         header, mode = CallUrlUtil.get_request_message(retry_num, repeat_num, page_index, return_url, total_page, full_file_name, header, mode)

#                         if response_code == 200 and 'NO_DATA' not in response.text:
#                             print("response_code!!!!!!", response_code)
#                             json_data = response.json()
                            
#                             json_file_name = os.path.join(full_file_path, f"{source_file_name}.json")
#                             with open(json_file_name, 'w', encoding='utf-8') as f:
#                                 f.write(response.text)

#                             # json 읽어오기
#                             result = CallUrlUtil.read_json(json_data, dtst_cd)
#                             result_json = result['result_json_array']
#                             result_size = len(result_json)

#                             # 데이터 존재 시
#                             if result_size != 0:
#                                 retry_num = 0  # 재시도 횟수 초기화
#                                 if page_index == 1: # 첫 페이지일 때
#                                     # 페이징 계산
#                                     total_count = int(result['total_count'])
#                                     total_page = CallUrlUtil.get_total_page(total_count, result_size)

#                                 extracted_data = []
#                                 for item in json_data.get('data', []):
                                    
#                                     # meta_urls에 값 담기
#                                     meta_url = item.get('meta_url')
#                                     if meta_url:
#                                         meta_urls.append(meta_url)
#                                     extracted_data.append({
#                                         # 'core_data_nm' : item.get('core_data_nm') if item.get('core_data_nm') is not None else '',
#                                         'core_data_nm' : item.get('core_data_nm'),
#                                         'cost_unit' : item.get('cost_unit'),
#                                         'created_at' : item.get('created_at'), 
#                                         'data_limit' : item.get('data_limit'),
#                                         'data_type' : item.get('data_type'),
#                                         'dept_nm' : item.get('dept_nm'),
#                                         '"desc"' : item.get('desc'),
#                                         'download_cnt' : item.get('download_cnt'),
#                                         'etc' : item.get('etc'),
#                                         'ext' : item.get('ext'),
#                                         'id' : item.get('id'),
#                                         'is_charged' : item.get('is_charged'),
#                                         'is_copyrighted' : item.get('is_copyrighted'),
#                                         'is_core_data' : item.get('is_core_data'),
#                                         'is_deleted' : item.get('is_deleted'),
#                                         'is_list_deleted' : item.get('is_list_deleted'),
#                                         'is_std_data' : item.get('is_std_data'),
#                                         'is_third_party_copyrighted' : item.get('is_third_party_copyrighted'),
#                                         'keywords' : item.get('keywords'),
#                                         'list_id' : item.get('list_id'),
#                                         'list_title' : item.get('list_title'),
#                                         'media_cnt' : item.get('media_cnt'),
#                                         'media_type' : item.get('media_type'),
#                                         'meta_url' : item.get('meta_url'),
#                                         'new_category_cd' : item.get('new_category_cd'),
#                                         'new_category_nm' : item.get('new_category_nm'),
#                                         'next_registration_date' : item.get('next_registration_date'),
#                                         'org_cd' : item.get('org_cd'),
#                                         'org_nm' : item.get('org_nm'),
#                                         'ownership_grounds' : item.get('ownership_grounds'),
#                                         'regist_type' : item.get('regist_type'),
#                                         'register_status' : item.get('register_status'),
#                                         'share_scope_nm' : item.get('share_scope_nm'),
#                                         'title' : item.get('title'),
#                                         'update_cycle' : item.get('update_cycle'),
#                                         'updated_at' : item.get('updated_at'),

#                                         # 필요한 모든 필드를 추가
#                                     })

#                                 with session.begin() as conn:
#                                     for row in extracted_data:
#                                         columns = ', '.join(row.keys())
#                                         # values = ', '.join([f"'{v}'" for v in row.values()])
#                                         values = ', '.join([f"'{v}'" if v is not None else 'NULL' for v in row.values()])
#                                         insert_stmt = f'''
#                                             INSERT INTO {table_name} ({columns}) VALUES ({values});
#                                         '''
#                                         conn.execute(insert_stmt)

#                                 row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
#                                 if row_count == 0:
#                                     header = True
#                                     mode = "w"

#                                 # csv 파일 생성
#                                 CallUrlUtil.create_csv_file(link_file_sprtr, '202407', '1', full_file_path, file_name, result_json, header, mode, page_index)

#                             row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)  # 행 개수 확인
#                             if row_count != 0:
#                                 logging.info(f"현재까지 파일 내 행 개수: {row_count}")
#                             if total_page == 1:
#                                 repeat_num += 1
#                                 break
#                             else:
#                                 if page_index < total_page:
#                                     page_index += 1
#                                 elif page_index == total_page:
#                                     if params_len == 1:
#                                         repeat_num += 1
#                                         break
#                                     elif params_len != 1:
#                                         if repeat_num < params_len:
#                                             page_index = 1
#                                             repeat_num += 1
#                                         else:
#                                             repeat_num += 1
#                                             break
#                         else:
#                             logging.info(f"call_url resultmsg::: NO_DATA")
#                             retry_num += 1
#                             continue
#             except AirflowSkipException as e:
#                 raise e
#             except Exception as e:
#                 logging.info(f"call_url Exception::: {e}")
#                 raise e
            
#             return meta_urls
        
#         @task
#         def process_meta_urls(meta_urls,**kwargs):
#             print("hello_file!!!!!!!!")
#             import requests
#             # file_url에 있는 meta_url을 담은 list에 있는 url를 반복작업
#             for url in meta_urls:
#                 if url:
#                     response = requests.get(url, verify=False)
#                     if response.status_code == 200:
#                         json_data = response.json()
#                         download_url = json_data.get('url')
#                         if download_url:
#                             table_name = TdmFileUrlInfo.__tablename__
#                             with session.begin() as conn:
#                                 insert_stmt = f'''
#                                     UPDATE {table_name}
#                                     SET download_url = '{download_url}'
#                                     WHERE id = (SELECT id FROM {table_name} WHERE meta_url = '{url}' LIMIT 1);
#                                 '''
#                                 conn.execute(insert_stmt)
#                         logging.info(f"Meta URL response: {json_data}")
#                     else:
#                         logging.error(f"Failed to fetch meta URL {url} with status code {response.status_code}")

#         @task
#         def insert_bsc_info(**kwargs):
#             """
#             tn_data_bsc_info_test 테이블에 tdm_list_url_info의 category_nm과 tdm_file_url_info의 download_url 값을 업데이트합니다.
#             """
#             insert_bsc_info_stmt = '''
#                 INSERT INTO tn_data_bsc_info_test (dtst_cd, dtst_nm, category_nm, download_url)
#                 WITH max_sn AS (
#                     SELECT COALESCE(MAX(sn), 0) AS max_sn
#                     FROM tn_data_bsc_info_test
#                 )
#                 SELECT 
#                     'data' || (SELECT max_sn FROM max_sn) + ROW_NUMBER() OVER (ORDER BY l.id),
#                     'test', 
#                     l.category_nm, 
#                     f.download_url
#                 FROM 
#                     tdm_list_url_info l
#                 JOIN 
#                     (SELECT 
#                         list_id, created_at,
#                         download_url, 
#                         ROW_NUMBER() OVER (PARTITION BY list_id ORDER BY created_at DESC) as rn
#                     FROM 
#                         tdm_file_url_info) f ON l.id = f.list_id AND f.rn = 1
#                 GROUP BY 
#                     l.category_nm, 
#                     f.download_url, 
#                     l.id;
#             '''
#             try:
#                 with session.begin() as conn:
#                     conn.execute(insert_bsc_info_stmt)
#                 logging.info(f"insert_bsc_info completed successfully")
#             except Exception as e:
#                 logging.error(f"insert_bsc_info Exception::: {e}")
#                 raise e    

#         # call_url()  
#         @task
#         def check_loading_result(collect_data_list):
#             """
#             DW 적재 결과 확인
#             params: collect_data_list
#             """
#             # tdm_list_url_info = TdmListUrlInfo(**collect_data_list['tdm_list_url_info'])
#             # dw_tbl_phys_nm = TdmListUrlInfo.dw_tbl_phys_nm
#             # tn_data_bsc_info_test = TnDataBscInfo(**collect_data_list['tn_data_bsc_info_test'])
#             dw_tbl_phys_nm = TnDataBscInfo.__tablename__

#             result_count = 0
#             get_count_stmt = f"""SELECT COUNT(*) FROM {dw_tbl_phys_nm} """
#             try:
#                 with session.begin() as conn:
#                     result_count = conn.execute(get_count_stmt).first()[0]
#                     TnDataBscInfo.id = result_count
#                     logging.info(f"check_loading_result id::: {result_count}")
#             except Exception as e:
#                 logging.error(f"check_loading_result Exception::: {e}")
#                 raise e

#         file_path = create_directory(collect_data_list)
#         meta_url = file_url_call(collect_data_list)    
#         file_path >> call_url(collect_data_list, file_path) >> meta_url >> process_meta_urls(meta_url) >> insert_bsc_info() >> check_loading_result(collect_data_list)
            
#     collect_data_list = collect_data_info()
#     call_url_process.expand(collect_data_list = collect_data_list)

# dag_object = insert_tdm_list_url()


# # only run if the module is the main program
# if __name__ == "__main__":
#     conn_path = "../connections_minio_pg.yaml"
#     # variables_path = "../variables.yaml"
#     dtst_cd = ""

#     dag_object.test(
#         execution_date=datetime(2023,12,28,15,00),
#         conn_file_path=conn_path,
#         # variable_file_path=variables_path,
#         # run_conf={"dtst_cd": dtst_cd},
#     )
