import os
import pandas as pd
import logging
from math import trunc
import json
import re

import requests

from util.date_custom_util import DateUtil
from util.file_util import FileUtil
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
# import 

class CallUrlUtil:
    def read_json(json_data, pvdr_site_cd, pvdr_inst_cd, dtst_cd, add_column):
        """
        json_data 를 search_keyword, list_keyword에 따라 parsing
        params: json_data
        return: result_json_array, total_count (json파일에 나와있는 데이터 총 개수)
        """
        list_keywords = CallUrlUtil.set_keyword("list_keywords", pvdr_site_cd, pvdr_inst_cd, dtst_cd)
        search_keyword = CallUrlUtil.set_keyword("search_keyword", pvdr_site_cd, pvdr_inst_cd, dtst_cd)
        search_result = {}
        result_json_array = []  # list_keywords 에 해당하는 데이터 리스트
        total_count = 0
        ignore_column = CallUrlUtil.get_ignore_column(dtst_cd)
        result_json_array = CallUrlUtil.recursive_json_for_keyword(json_data, list_keywords, search_keyword, search_result, result_json_array,ignore_column, add_column, dtst_cd)

        if len(result_json_array) != 0:
            if search_keyword == "":
                total_count = len(result_json_array)
            else:
                total_count = search_result.get(search_keyword)

            if dtst_cd == "data677":  # 새올_민원통계
                total_count = result_json_array[0]['tot_cnt']
        return {
            'result_json_array' : result_json_array,
            'total_count' : total_count
            }
    
    def get_ignore_column(dtst_cd):
        """
        dtst_cd별 수집하지 않을 key 설정
        params: dtst_cd
        return: ignore_column
        """
        ignore_column = {
                        "data778" : "callNumbers",
                        }.get(dtst_cd, "")
        return ignore_column
    
    def recursive_json_for_keyword(json_data, list_keywords, search_keyword, search_result, result_json_array,ignore_column, add_column,dtst_cd):
        # # json 배열인 경우
        if isinstance(json_data, list):
            for item in json_data:
                # json 객체인 경우
                if (isinstance(item, dict)):
                    # list_keywords 가 없는 경우 result_json_array 에 담음
                    if (list_keywords == ""):
                        temp_dict = {}
                        for key, values in item.items():
                            temp_dict = CallUrlUtil.parsing_value(temp_dict, key, values, ignore_column)
                        result_json_array.append(temp_dict)
                    else:
                        # 재귀적으로 하위 객체에서 데이터를 추출하여 배열에 담음
                        CallUrlUtil.recursive_json_for_keyword(item, list_keywords, search_keyword, search_result, result_json_array,ignore_column, add_column, dtst_cd)
        # json 객체인 경우
        if isinstance(json_data, dict):
            # list_keyword 에 해당하는 경우
            for list_keyword in list_keywords:
                if list_keyword in json_data:
                    value = json_data.get(list_keyword)
                    if isinstance(value, dict):  # 객체일 경우
                        temp_dict = {}
                        for key, values in value.items():
                            if ignore_column == key:
                                continue
                            temp_dict = CallUrlUtil.parsing_value(temp_dict, key, values,ignore_column)
                        result_json_array.append(temp_dict)
                    elif isinstance(value, list):  # 배열일 경우
                        for item in value:
                            temp_dict = {}
                            for key, values in item.items():
                                temp_dict = CallUrlUtil.parsing_value(temp_dict, key, values,ignore_column)
                                if len(list_keywords) > 1:  # 도서관별 인기대출도서 통합 예외 (list_keywords 컬럼 구분 값 추가), 신문고 민원
                                    add_dict = {add_column : list_keyword}
                                    temp_dict.update(add_dict)
                            result_json_array.append(temp_dict)
            # 재귀적으로 하위 객체에서 데이터를 추출하여 배열에 담음
            for key, value in json_data.items():
                if (isinstance(value, dict) or isinstance(value, list)) and (key not in list_keywords):
                    CallUrlUtil.recursive_json_for_keyword(value, list_keywords, search_keyword, search_result, result_json_array,ignore_column, add_column, dtst_cd)
                # search_keyword 에 해당하는 값 search_result 에 담기
                if search_keyword == key:
                    search_result[key] = value
        return result_json_array
    
    def parsing_value(temp_dict, key, values, ignore_column):
        """
        list_keyword에 해당하는 데이터 내 json 형식이 존재하는 경우 parsing
        parmas: temp_dict, key, values, ignore_column
        return: temp_dict
        """
        # 데이터 내 "key" : [ values ] 존재하는 경우
        if isinstance(values, list):
            for list_value in values:
                # values 가 dict인 경우 "key" : [ {'dict_key': dict_value } ]
                if isinstance(list_value, dict):
                    for dict_key, dict_value in list_value.items():
                        if ignore_column == dict_key:
                            continue
                        # dict_value가 dict 인 경우 "key" : [ {'dict_key': {'key1':'val1', 'key2':'val2'} ]
                        if isinstance(dict_value, dict):
                            temp_dict.update(dict_value)  # value에 dict_value update
                        # dict_value가 list 인 경우 "key" : [ {'dict_key': ['val1','val2','val3']} ]
                        else:
                            temp_dict.update(list_value)
                # values 가 list인 경우 "key" : [ 'val1','val2','val3' ]
                else:
                    for list_value in values:
                        temp_dict[key] = ', '.join(f"{s}" for s in values)
        # 데이터 내 "key" : { values } 존재하는 경우
        elif isinstance(values, dict):
            temp_dict.update(values)
        else:
            temp_dict[key] = values
        return temp_dict

    # 데이터 정보
    def set_keyword(keyword, pvdr_site_cd, pvdr_inst_cd, dtst_cd):
        """
        dtst_cd별 list_keywords, search_keyword 설정
        params: keyword
        return: return_keyword (list_keywords, search_keyword에 해당하는 키워드 값)
        """
        if keyword == "list_keywords":  # 데이터를 담고 있는 keySet
            if pvdr_inst_cd in {"pi00001"} or dtst_cd in {"data39", "data54"} or pvdr_site_cd == "ps00026": #지방재정
                return ["row"]
            if dtst_cd in {"data919","data920","data922","data6"}:
                return ["data"]
            if pvdr_inst_cd in {"pi00008", "pi00012", "pi00019"} or dtst_cd in {"data852", "data50", "data650", "data787", "data788", "data853", "data1059"}:
                return ["item"]
            if dtst_cd == "data6" or pvdr_site_cd == "ps00029":
                return ["data"]
            if pvdr_inst_cd in {"pi00004", "pi00022"} or dtst_cd in { "data4", "data5", "data22", "data26", "data704"}:
                return ["items"]  # 공공데이터활용지원센터/한국환경공단 or 5분소통, 사고정보, 용수시설, 보안등, 교통량_VDS
            if dtst_cd in {"data778", "data780"}:
                return ["doc"]
            if dtst_cd == "data777": # 정보나루_정보공개_도서관조회
                return ["lib"]
            if dtst_cd in {"data677", "data762", "data763"}: # 새올 민원 , 직원정보, 부서정보
                return ["list"]
            if dtst_cd in {"datadata1011"}:
                return ["msgBody"]
            if dtst_cd == "data53":
                return ["kinderInfo"]
            if dtst_cd in {"data695", "data851"}: # 인사 - 분류코드/ 임용발령
                return ["esbResultData"]
            if (pvdr_site_cd == "ps00028" and dtst_cd not in {"data695", "data694", "data851"}): # 차세대인사_초과근무정산내역/출장정보/당해년도연가일수통계/연가외_휴가신청목록
                return ["resultList"]
            if dtst_cd in {"data675"}:  # 국민 신문고
                return ["Petition","Receipt","Process"]
            
        if keyword == "search_keyword":  # 데이터 건수 keySet
            if pvdr_inst_cd in {"pi00001"} or dtst_cd in {"data39", "data54"} or pvdr_site_cd == "ps00026":
                return "list_total_count"
            if dtst_cd == "data6": #CCTV_영상정보
                return "datacount"
            if dtst_cd in {"data919","data920","data922"}: # 공공데이터 포털 (목록/파일/오픈API)
                return "matchCount"
            if pvdr_inst_cd in {"pi00004", "pi00012", "pi00022"} or (pvdr_inst_cd == "pi00008" and dtst_cd == "data49") or (pvdr_inst_cd == "pi00009" and dtst_cd != "data6") \
                or dtst_cd in {"data852", "data22", "data26", "data51", "data4", "data650", "data678", "data704", "data706", "data787", "data788", "data853", "data1059"}:
                return "totalCount"
            if dtst_cd in {"data777"}: # 정보나루_정보공개_도서관조회
                return "numFound"
            # if dtst_cd == "data780":  #정보나루_도서관_지역별_인기대출_도서
            #     return "resultNum"
            if pvdr_site_cd == "ps00029": # 차세대 지방재정
                return "totalCnt"
        return ""

    # url에 설정하는 파라미터
    def set_params(tn_data_bsc_info, session, start_date, end_date, kwargs):
        """
        pvdr_site_cd, pvdr_inst_cd, dtst_cd 별 파라미터 및 파라미터 길이 설정
        params: tn_data_bsc_info, session, start_date (처리 데이터의 날짜 (데이터 기준 시점)), end_date (실제 실행하는 날짜를 KST 로 설정), kwargs
        return: params, params_len
        """
        dtst_cd = tn_data_bsc_info.dtst_cd
        pvdr_site_cd = tn_data_bsc_info.pvdr_site_cd
        pvdr_inst_cd = tn_data_bsc_info.pvdr_inst_cd
        rfrnc_phys_tbl_nm = tn_data_bsc_info.rfrnc_phys_tbl_nm  # 참조할 물리 테이블명
        rfrnc_col_nm = tn_data_bsc_info.rfrnc_col_nm  # 참조할 컬럼명

        start_date_3month = start_date.set(day=1).add(months=-3)  # 처리 데이터의 날짜 (데이터 기준 시점) 세 달 전
        year = start_date.year
        yyyymm = start_date.strftime("%Y%m")
        yyyymm_dash = start_date.strftime("%Y-%m")
        today = start_date.strftime("%Y%m%d")
        start_date_dash = start_date.strftime("%Y-%m-%d")
        end_date_dash = end_date.set(day=1).add(days=-1).strftime("%Y-%m-%d")
        date_list = DateUtil.get_date_list(start_date_3month, end_date)  # 3month_date_list - 지방재정365(계약현황, 세부사업별_세출현황)

        param_list = CallUrlUtil.set_param_list(dtst_cd, rfrnc_phys_tbl_nm, session, rfrnc_col_nm, kwargs)
        params_dict = {}

        # 파라미터 존재 시
        if dtst_cd in {"data852"}:  # 기상청_단기예보_시간
            params_dict["params"] = [today, start_date.strftime("%H") + '00']
        if dtst_cd in {"data677", "data32"}:  # 새올_민원통계, 측정소별_실시간_일평균_정보_조회
            params_dict["params"] = today
        # TAAS, 지방재정365(계약현황, 세부사업별_세출현황 제외), 한국천문연구원_특일_정보, 인사(당해년도연가일수통계_정보송신), 국가통계포털(노령화지수_시도, 추계인구_시_군_구), 지방재정(사업, 세부사업, 부서코드, 부서코드이력, 세출통계목코드, 회계구분코드, 세입목코드 제외)
        if (pvdr_inst_cd == "pi00012") or (pvdr_site_cd == "ps00026" and dtst_cd not in {"data652", "data659"}) or dtst_cd in {"data699", "data59", "data66", "data650"}\
            or (pvdr_site_cd == "ps00029" and dtst_cd not in {"data680", "data682", "data683", "data849", "data850", "data873"}):
            params_dict["params"] = year
        if dtst_cd in {"data33"}:  # 측정소별_실시간_월평균_정보_조회
            params_dict["params"] = yyyymm
        if dtst_cd in {"data652", "data659"}:  # 지방재정365(계약현황, 세부사업별_세출현황)
            param_list = date_list
            params_dict["param_list"] = param_list
        if dtst_cd == "data695":  # 인사(인사_분류코드정보_송신)
            params_dict["params"] = "ALL"
        if dtst_cd == "data696":  # 인사(초과근무정산내역_정보송신)
            params_dict["params"] = [start_date_3month.strftime("%Y%m%d"), end_date.set(day=1).add(days=-1).strftime("%Y%m%d")]
            params_dict["param_list"] = param_list
        if dtst_cd == "data851":  # 인사(인사_임용발령정보 송신)
            params_dict["params"] = [start_date.strftime("%Y%m%d"), end_date.set(day=1).add(days=-1).strftime("%Y%m%d")]
        if dtst_cd in {"data697", "data698"}:  # 인사(출장정보_송신, 연가신청목록_정보송신)
            params_dict["params"] = today
        if dtst_cd == "data700":  # 인사(연가외_휴가신청목록_정보송신)
            params_dict["params"] = today
            param_list = [2,3,4,5]
            params_dict["param_list"] = param_list  # 주의 연가 외 휴가목록(1은 안됨)
        
 
        
        # 파라미터가 없을 때
        params_len = 1
        if param_list != {}:
            params_dict["param_list"] = param_list
            params_len = len(params_dict.get("param_list"))
        return params_dict, params_len

    def set_param_list(dtst_cd, rfrnc_phys_tbl_nm, session, rfrnc_col_nm, kwargs):
        """
        param_list 설정requests
        params: dtst_cd, rfrnc_phys_tbl_nm 참조할 테이블명, session, rfrnc_col_nm 참조할 컬럼명, kwargs
        return: param_list
        """
        param_list = {}
        param_lists = json.loads(kwargs['var']['value'].param_lists)
        for key in param_lists.keys():
            if dtst_cd == key:
                param_list = param_lists.get(key)

        if rfrnc_col_nm != None and rfrnc_col_nm != '' and rfrnc_phys_tbl_nm != None and rfrnc_phys_tbl_nm != '':
            column_val = rfrnc_col_nm.split(',')
            for i in range(len(column_val)):
                globals()['column' + str(i + 1)] = column_val[i]
            column1 = globals()['column1']

            select_seq_stmt = f"""
                SELECT STRING_AGG("{column1}",',') "{column1}"
                FROM {rfrnc_phys_tbl_nm}
            """
            select_data_crtr_pnttm_stmt = f"""
                        WHERE data_crtr_pnttm = (
                            SELECT data_crtr_pnttm 
                            FROM {rfrnc_phys_tbl_nm}
                            GROUP BY data_crtr_pnttm
                            ORDER BY data_crtr_pnttm DESC
                            LIMIT 1)
                        """
            if dtst_cd in {"data762", "data778", "data781", "data782", "data784", "data856"}:
                # 직원정보 - 부서정보 dep_code 조회
                # 도서관_장서_대출_조회, 도서관별 대출반납추이_일단위, 도서관별 대출반납추이_시간단위, 도서관별_인기대출도서_통합 - 정보 공개 도서관 libCode 조회
                select_seq_stmt += f"""
                                    {select_data_crtr_pnttm_stmt}
                                    """
            if dtst_cd == "data50":  # 상세_정보조회
                # 사업장_정보조회_서비스 seq 조회
                # 가장 최근 data_crtr_pnttm, datacrtym
                column2 = globals()['column2']
                select_seq_stmt += f"""
                                    {select_data_crtr_pnttm_stmt}
                                    AND "{column2}" = (
                                        SELECT "{column2}"
                                        FROM {rfrnc_phys_tbl_nm}
                                        GROUP BY "{column2}"
                                        ORDER BY "{column2}" DESC
                                        LIMIT 1)
                                    """
            if dtst_cd == "data696":  # 초과근무정산내역_정보송신
                # 부서정보 dep_code 조회
                select_seq_stmt = f"""
                                    SELECT STRING_AGG(SUBSTRING("{column1}", 1, 9),',') "{column1}"
                                    FROM {rfrnc_phys_tbl_nm}
                                    {select_data_crtr_pnttm_stmt}
                                    """
            if dtst_cd == "data779":  # 도서_상세_조회
                # 도서관별 장서/대출 데이터 isbn13 조회
                # 가장 최근 data_crtr_pnttm으로 GROUP BY isbb13
                select_seq_stmt = f"""
                                    SELECT STRING_AGG({column1},',') {column1}
                                    FROM (
                                        SELECT {column1}
                                        FROM {rfrnc_phys_tbl_nm}
                                        {select_data_crtr_pnttm_stmt}
                                        GROUP BY {column1}
                                    ) b
                                    """
            try:
                with session.begin() as conn:
                    for dict_row in conn.execute(select_seq_stmt).all():
                        # param_list = dict_row[0].split(',')
                        # dict_row[0]이 None이 아닌지 확인
                        if dict_row[0] is not None:
                            param_list = dict_row[0].split(',')
                        else:
                            param_list = []  # None인 경우 빈 리스트 할당
            except Exception as e:
                logging.info(f"set_params Exception::: {e}")
                raise e

        # if dtst_cd == "data780":  # 도서관_지역별_인기대출_도서_조회
        #     list1 = [11,31,31260]
        #     list2 = [0,1,2]
        #     list3 = [0,6,8,14,20,30,40,50,60,-1]
        #     param_list = [f"{x},{y},{z}" for x in list1 for y in list2 for z in list3]
        return param_list


    def set_url(dtst_cd, link_se_cd, pvdr_site_cd, pvdr_inst_cd, params_dict, repeat_num, page_no):
        """
        dtst_cd 별 url 설정
        params: dtst_cd, params_dict, repeat_num, page_no
        return: return_url
        """
        if params_dict != {}:
            params = params_dict.get("params")
            param_list = params_dict.get("param_list")
        
        if dtst_cd in {"data919","data920","data922"}:
            return f"{page_no}"
        if dtst_cd in {"data1059"}:  # 사회복지시설표준데이터
            return f"{page_no}"
        if dtst_cd == "data921":
            return f"{param_list[repeat_num - 1]}&pageNo={page_no}"
        if dtst_cd in {"data852"}:  # 기상청_단기예보_시간
            return f"{page_no}&base_date={params[0]}&base_time={params[1]}"
        if dtst_cd == "data19":  # 건축물대장_표제부
            return f"{page_no}&bjdongCd={param_list[repeat_num - 1]}"
        if dtst_cd in {"data30", "data31"}:  # 대기오염정보_측정소정보, 대기오염정보_측정소별_실시간_측정정보_조회
            return f"{page_no}&stationName={param_list[repeat_num - 1]}"
        if dtst_cd == "data32":  # 대기오염_국가측정망_실시간_일평균_정보_조회 
            return f"{page_no}&msrstnName={param_list[repeat_num - 1]}&inqBginDt={params}&inqEndDt={params}"
        if pvdr_inst_cd == "pi00012" and dtst_cd != "data787":  # TAAS 교통사고분석시스템 - 사망교통사고정보 제외
            return f"{page_no}&searchYearCd={params}"
        if dtst_cd == "data787":  # TAAS 교통사고분석시스템 - 사망교통사고정보
            return f"{page_no}&searchYear={params}"
        if pvdr_site_cd == "ps00026" and dtst_cd not in {"data652", "data659"}:  # 지방재정365(계약현황, 세부사업별_세출현황 제외)
            return f"{page_no}&fyr={params}"
        if dtst_cd == "data652":  # 지방재정365(계약현황)
            return f"{page_no}&smz_ctrt_ymd={param_list[repeat_num - 1]}"
        if dtst_cd == "data659":  # 지방재정365(세부사업별_세출현황)
            return f"{page_no}&fyr={param_list[repeat_num - 1][:4]}&exe_ymd={param_list[repeat_num - 1]}"
        if pvdr_inst_cd == "pi00019" or (pvdr_site_cd == "ps00010" and dtst_cd not in {"data59", "data66"}) or pvdr_site_cd == "ps00008" or dtst_cd in {"data4", "data8", "data704", "data706", "data53", "data854", "data855"}:  # 국가통계포털(노령화지수_시도, 추계인구_시_군_구 제외)
            return ""
        if dtst_cd in {"data59", "data66", "data650"}:  # 국가통계포털(노령화지수_시도, 추계인구_시_군_구), 한국천문연구원_특일_정보
            return f"{params}"
        if dtst_cd == "data33":  # 대기오염_국가측정망_월평균_측정정보_조회 
            return f"{page_no}&msrstnName={param_list[repeat_num - 1]}&inqBginMm={params}&inqEndMm={params}"
        if dtst_cd == "data786":  # 지역별 독서량_독서율
            return f"{params[:4]}&month={params[-2:]}"
        if link_se_cd == "new" and dtst_cd not in {"data978"}: #link_se_cd 연계구분코드 new인 데이터 중에서 dtst_cd가 data978이 아닌 데이터셋
            return ""
        else:
            return f"{page_no}"
        #     return f"{param_list[repeat_num - 1]}&pageNo={page_no}"



    def get_total_page(total_count, result_size):
        """
        총 페이지 수 계산
        params: total_count, result_size
        return: total_page
        """
        total_page = trunc(total_count / result_size)  # 몫
        remainder = total_count % result_size  # 나머지

        if (remainder != 0):
            total_page += 1
        logging.info(f"총 데이터 건수: {total_count}, 총 페이지 수: {total_page}")
        return total_page
    
    @staticmethod
    def get_request_message(retry_num, repeat_num, page_no, return_url, total_page, full_file_name, header, mode):
    
        logging.info(f"[DEBUG] Inside get_request_message: retry_num={retry_num}, repeat_num={repeat_num}, page_no={page_no}, return_url={return_url}, total_page={total_page}, full_file_name={full_file_name}, header={header}, mode={mode}")
        if retry_num == 0 and repeat_num == 1 and page_no == 1:  # 첫 호출
            if full_file_name is not None:
                if os.path.exists(full_file_name):
                    os.remove(full_file_name)
            logging.info(f"호출 url: {return_url}")
        elif retry_num != 0:  # 재호출
            logging.info(f"호출 결과 없음, url: {return_url} 로 재호출, 재시도 횟수: {retry_num}")
        else:  # 반복 횟수만큼 호출, 총 페이지 개수만큼 호출
            header = False
            mode = "a"  # 파일 쓰기 모드 append
            url_message = ""
            if page_no != 1:
                url_message = f" ({page_no}/{total_page})페이지 "
            logging.info(f"{repeat_num}번째{url_message}호출 url: {return_url}")
        return header, mode
        
    def create_csv_file(link_file_sprtr, data_crtr_pnttm, clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no):
        """
        공통 헤더 컬럼, 값 추가 및 csv 파일 생성
        params: link_file_sprtr, data_crtr_pnttm, clct_log_sn, full_file_path, file_name, result_json, header, mode, page_no
        """
        # 공통 헤더 컬럼, 값 추가
        common_dict = {"data_crtr_pnttm" : data_crtr_pnttm, "clct_pnttm" : DateUtil.get_ymdhm(), "clct_log_sn" : clct_log_sn, "page_no" : page_no}
        for dict_value in result_json:
            dict_value.update(common_dict)

        # csv 파일 생성
        try:
            # os.chdir(full_file_path)
             # 디렉토리 존재 확인 및 이동
            if not os.path.exists(full_file_path):
                os.makedirs(full_file_path)
            os.chdir(full_file_path)

            df = pd.json_normalize(result_json, sep= "_")
            # 데이터프레임 비어 있는지 확인
            if df.empty:
                logging.error("데이터프레임이 비어 있음. CSV 파일 생성 중단.")
                raise ValueError("데이터프레임이 비어 있음. CSV 파일 생성 불가.")
            df = df.replace("\n"," ", regex=True).replace("\r\n"," ", regex=True).replace("\r"," ", regex=True).apply(lambda x: (x.str.strip() if x.dtypes == 'object' and x.str._inferred_dtype == 'string' else x), axis = 0)  # 개행문자 제거, string 양 끝 공백 제거
            
            full_file_name = full_file_path + file_name
            # 5분_소통정보, 대기오염정보_측정소별_실시간_측정정보_조회(->대기오염_국가측정망_시간대별_측정정보), 실시간_측정정보_조회(->대기오염_자체측정망_시간대별_측정정보) - clct_sn 생성하지않음
            if '5분_소통정보' in file_name or '대기오염_국가측정망_시간대별_측정정보' in file_name or '대기오염_자체측정망_시간대별_측정정보' in file_name:
                df.to_csv(full_file_name, sep= link_file_sprtr, header= header, index=False, mode= mode, encoding='utf-8-sig')
            else:
                # clct_sn 로그 순번 설정
                # row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)
                row_count = 0
                if os.path.exists(full_file_name):
                    try:
                        row_count = FileUtil.check_csv_length(link_file_sprtr, full_file_name)
                    except Exception as e:
                        logging.warning(f"기존 CSV 파일의 행 개수 확인 실패. 초기값 사용: {e}")
                
                df.index += row_count + 1
                if '신문고민원_접수' in file_name:
                    selected_columns = df.iloc[:, :34]
                    selected_columns.to_csv(full_file_name, sep= link_file_sprtr, header= header, index_label= "clct_sn", mode= mode, encoding='utf-8-sig')
                else:# csv 헤더 읽어들여 컬럼 순서 지정
                    if row_count != 0:
                        column_order = pd.read_csv(full_file_name, sep= link_file_sprtr, low_memory = False).columns.drop('clct_sn')
                        df = df.reindex(columns=column_order, fill_value=None)
                        # df = df[column_order]
                        #print("@@@@@@df : ",df)
                    df.to_csv(full_file_name, sep=link_file_sprtr, header=header, index_label="clct_sn", mode=mode, encoding='utf-8-sig')
        # 파일 생성 여부 확인
            if not os.path.exists(full_file_name):
                logging.error(f"CSV 파일 생성 실패: {full_file_name}")
                raise FileNotFoundError(f"CSV 파일 생성 실패: {full_file_name}")
            else:
                logging.info(f"CSV 파일 생성 성공: {full_file_name}")

        except PermissionError:
            logging.error(f"파일 쓰기 권한이 없습니다: {full_file_name}")
            raise
        except Exception as e:
            logging.error(f"create_csv_file Exception::: {e}")
            raise
        
    def create_source_file(json_data, source_file_name, full_file_path, mode):
        """
        원천 파일 생성
        params: json_data, source_file_name, full_file_path, mode
        """
        try:
            os.chdir(full_file_path)
            df = pd.json_normalize(json_data)
            df.to_json(source_file_name, mode= mode, force_ascii= False, orient= 'records', lines= True)
        except Exception as e:
            logging.info(f"create_source_file Exception::: {e}")
            raise e
    
    def insert_fail_history_log(th_data_clct_mastr_log, return_url, file_path, session, params, pgng_cnt):
        """
        다운로드 실패 시 th_data_clct_contact_fail_hstry_log 테이블에 입력
        params: th_data_clct_mastr_log, return_url, file_path, session, params, pgng_cnt
        """
        from dto.th_data_clct_contact_fail_hstry_log import ThDataClctCallFailrHistLog
        from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST
        from pendulum import now

        try:
            with session.begin() as conn:
                th_data_clct_contact_fail_hstry_log = ThDataClctCallFailrHistLog()
                th_data_clct_contact_fail_hstry_log.clct_log_sn = th_data_clct_mastr_log.clct_log_sn
                th_data_clct_contact_fail_hstry_log.dtst_cd = th_data_clct_mastr_log.dtst_cd
                th_data_clct_contact_fail_hstry_log.clct_fail_url = return_url
                th_data_clct_contact_fail_hstry_log.clct_pgng_no = pgng_cnt
                th_data_clct_contact_fail_hstry_log.stts_cd = CONST.STTS_ERROR
                th_data_clct_contact_fail_hstry_log.stts_msg = 'OpenAPI_ServiceResponse'  # test
                th_data_clct_contact_fail_hstry_log.crt_dt = now(tz="UTC")
                th_data_clct_contact_fail_hstry_log.estn_field_one = params
                th_data_clct_contact_fail_hstry_log.estn_field_two = file_path
                conn.add(th_data_clct_contact_fail_hstry_log)
        except Exception as e:
            logging.info(f"insert_th_data_clct_contact_fail_hstry_log Exception::: {e}")
            raise e
        
    
    def update_fail_history_log(th_data_clct_contact_fail_hstry_log, session, status_code, message):
        """
        th_data_clct_contact_fail_hstry_log 테이블 재시도 로그 업데이트
        params: th_data_clct_contact_fail_hstry_log, session
        """
        try:
            with session.begin() as conn:
                th_data_clct_contact_fail_hstry_log.stts_cd = status_code
                th_data_clct_contact_fail_hstry_log.stts_msg = message
                conn.merge(th_data_clct_contact_fail_hstry_log)
        except Exception as e:
            logging.info(f"update_fail_history_log Exception::: {e}")
            raise e
        logging.info(f"update_fail_history_log::: {th_data_clct_contact_fail_hstry_log.as_dict()}")
        
        
    def get_fail_data_count(clct_log_sn, session):
        """
        th_data_clct_contact_fail_hstry_log 테이블에서 실패 로그 개수 조회
        params: clct_log_sn, session
        return: fail_count
        """
        select_stmt = f'''
                        SELECT count(sn) 
                        FROM th_data_clct_contact_fail_hstry_log
                        WHERE 1=1
                            AND LOWER(stts_cd) = '{CONST.STTS_ERROR}'
                            AND clct_log_sn = '{clct_log_sn}'
                        '''
        with session.begin() as conn:
            for row in conn.execute(select_stmt).first():
                fail_count = row
        return fail_count
    
    def anonymize(value):
        """
        신문고, 새올_민원통계 데이터 비식별 처리
        """
        # 값이 있는 경우에만 비식별 처리를 진행
        if value:
            if len(value) == 1:
                # 문자열 길이가 1인 경우 전체를 '*'로 대체
                return '*'
            else:
                # 첫 번째 문자는 유지하고 나머지는 '*'로 대체
                return value[0] + re.sub(r'\S', '*', value[1:])
        else:
            return value
        
    def get_total_count(url,data_se_col_one,pvdr_data_se_vl_three,authKey,dtst_cd):
        """
        총 데이터 건수 조회 url 호출
        params: url, th_data_clct_mastr_log, session
        return: total_count
        """
        import xml.etree.ElementTree as ET
        
        # WSDL URL 및 서비스 엔드포인트
        service_url = url

          # 인증 정보
        systemid = 'data_gyeongsan'
        loginid = '78100900'
        deptCd = '5130234'
        authKey = authKey

        # 파라미터를 포함한 SOAP 요청 생성  
        if dtst_cd == "data1022":
            soap_request = f'''
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                            xmlns:bms="http://hamoni.mogaha.go.kr/bms"
                            xmlns:gov="java:gov.bms.lnk.ini.vo">
            <soapenv:Header/>
            <soapenv:Body>
                <bms:{data_se_col_one}>
                    <gov:login>
                        <gov:systemId>{systemid}</gov:systemId>
                        <gov:loginId>{loginid}</gov:loginId>
                        <gov:deptCd>{deptCd}</gov:deptCd>
                        <gov:authKey>{authKey}</gov:authKey>
                    </gov:login>
                    <gov:{pvdr_data_se_vl_three}/>
                </bms:{data_se_col_one}>
            </soapenv:Body>
            </soapenv:Envelope>
            '''
        else:
            systemCode = "datags"
            reqNum = 1
            
            soap_request = f'''
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                            xmlns:bms="http://hamoni.mogaha.go.kr/bms"
                            xmlns:gov="java:gov.bms.lnk.ini.vo">
            <soapenv:Header/>
            <soapenv:Body>
                <bms:{data_se_col_one}>
                    <gov:loginVo>
                        <gov:systemId>{systemid}</gov:systemId>
                        <gov:loginId>{loginid}</gov:loginId>
                        <gov:deptCd>{deptCd}</gov:deptCd>
                        <gov:authKey>{authKey}</gov:authKey>
                    </gov:loginVo>
                    <gov:{pvdr_data_se_vl_three}>
                        <gov:systemCode>{systemCode}</gov:systemCode>
                        <gov:reqNum>{reqNum}</gov:reqNum>
                    </gov:{pvdr_data_se_vl_three}>
                </bms:{data_se_col_one}>
            </soapenv:Body>
            </soapenv:Envelope>
            '''

        # 파라미터를 포함한 SOAP 요청 생성
        # soap_request = f'''
        # <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        #                   xmlns:bms="http://hamoni.mogaha.go.kr/bms"
        #                   xmlns:gov="java:gov.bms.lnk.ini.vo">
        #    <soapenv:Header/>
        #    <soapenv:Body>
        #       <bms:{data_se_col_one}>
        #          <gov:login>
        #             <gov:systemId>{systemid}</gov:systemId>
        #             <gov:loginId>{loginid}</gov:loginId>
        #             <gov:deptCd>{deptCd}</gov:deptCd>
        #             <gov:authKey>{authKey}</gov:authKey>
        #          </gov:login>
        #          <gov:{pvdr_data_se_vl_three}/>
        #       </bms:{data_se_col_one}>
        #    </soapenv:Body>
        # </soapenv:Envelope>
        # '''

        # HTTP 요청 헤더 설정
        headers = {
            'Content-Type': 'text/xml;charset=UTF-8'
        }

    
        try:
            # 요청 전 SOAP 데이터 확인
            logging.info(f"Sending SOAP request to URL: {service_url}")
            logging.info(f"SOAP Request:\n{soap_request}")
            # 서비스 호출
            response = requests.post(service_url, data=soap_request, headers=headers)
            response.raise_for_status()  # HTTP 에러 발생 시 예외 처리

            # 응답 출력
            logging.info(f"Response status code: {response.status_code}")
            logging.info(f"Response content:\n{response.text}")
            
            root = ET.fromstring(response.content)
            namespaces = {'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
                  'n1': 'http://hamoni.mogaha.go.kr/bms',
                  'ns2': 'java:gov.bms.lnk.ini.vo'}

            if dtst_cd == 'data1022':
                total_count_element = root.find('.//ns2:totalCnt', namespaces)
            else :
                total_count_element = root.find('.//ns2:totCnt', namespaces)

            if total_count_element is not None:
                total_count = int(total_count_element.text)
                logging.info(f"Total document count extracted: {total_count}")
                return total_count
            else:
                logging.warning("Failed to find 'totalCnt' in the SOAP response.")
                return 0

        except requests.exceptions.RequestException as e:
            logging.error(f"Error during SOAP request: {e}")
            return 0

        except ET.ParseError as e:
            logging.error(f"Error parsing SOAP response: {e}")
            return 0
