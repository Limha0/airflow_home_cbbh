import http.client
import logging
import random
import socket
import ssl

from urllib.parse import urlparse
from datetime import datetime

class OnnaraUtil:
    def make_message(dtst_cd, pvdr_data_se_vl_two, pvdr_data_se_vl_three, page_no):
        """
        dtst_cd별 data_list 및 message 생성
        params: dtst_cd, params_dict, repeat_num, page_no
        return: message, message_body
        """
        if dtst_cd == "data1022": # 문서등록대장_V2_부서별문서관리카드목록
            page_num = page_no
            data_list = f"""
                <gov:pageNum>{page_num}</gov:pageNum>
                """
        if dtst_cd in ('data1027','data1033'):  # 전체조직 목록,사용자목록 조회
                systemCode = "datags"
                reqNum = page_no
                data_list = f"""
                <gov:systemCode>{systemCode}</gov:systemCode>
                <gov:reqNum>{reqNum}</gov:reqNum>
                """
        # else:
        #     data_list = f"""<gov:pageNum>{page_num}</gov:pageNum>"""

        message_bms = f"""{pvdr_data_se_vl_two}"""
       
        message = f"""<gov:{pvdr_data_se_vl_three}>
                    {data_list}
            </gov:{pvdr_data_se_vl_three}>"""
        return message, message_bms
    
    def make_req_soap(dtst_cd, systemid, loginid, deptCd, authKey, message, message_bms):
        """
        soap 메세지 요청 작성
        params: if_id, src_org_cd, tgt_org_cd, msg_key, message
        return: soap_body
        """
        if dtst_cd == "data1022":
            soap_body = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                        xmlns:bms="http://hamoni.mogaha.go.kr/bms"
                        xmlns:gov="java:gov.bms.lnk.ini.vo">
                    <soapenv:Header/>
                    <soapenv:Body>
                        <bms:{message_bms}>
                            <gov:login>
                                <gov:systemId>{systemid}</gov:systemId>
                                <gov:loginId>{loginid}</gov:loginId>
                                <gov:deptCd>{deptCd}</gov:deptCd>
                                <gov:authKey>{authKey}</gov:authKey>
                            </gov:login>
                            {message}
                        </bms:{message_bms}>
                    </soapenv:Body>
                </soapenv:Envelope>
            """
        
        else:
            soap_body = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                        xmlns:bms="http://hamoni.mogaha.go.kr/bms"
                        xmlns:gov="java:gov.bms.lnk.ini.vo">
                    <soapenv:Header/>
                    <soapenv:Body>
                        <bms:{message_bms}>
                            <gov:loginVo>
                                <gov:systemId>{systemid}</gov:systemId>
                                <gov:loginId>{loginid}</gov:loginId>
                                <gov:deptCd>{deptCd}</gov:deptCd>
                                <gov:authKey>{authKey}</gov:authKey>
                            </gov:loginVo>
                            {message}
                        </bms:{message_bms}>
                    </soapenv:Body>
                </soapenv:Envelope>
            """
        return soap_body
    
    
    def send_http_request(addr, req_soap, charset):
        """
        http 요청
        params: addr, req_soap, charset
        return: response
        """
        connection = None
        response = None
        try:
            url_parts = urlparse(addr)
            path_with_query = f"{url_parts.path}?{url_parts.query}" if url_parts.query else url_parts.path
            
            # HTTPS or HTTP connection
            if url_parts.scheme == "https":
                connection = http.client.HTTPSConnection(url_parts.netloc, timeout=30)
            else:
                connection = http.client.HTTPConnection(url_parts.netloc, timeout=30)
            
            headers = {
                "Content-Type": f"text/xml; charset={charset}",
                "Accept": "application/xml, text/xml",
            }
            
            # Send request
            logging.info(f"SOAP 요청 전송: URL={addr}, Headers={headers}")
            connection.request("POST", path_with_query, req_soap.encode(charset), headers)
            raw_response = connection.getresponse()
            response = raw_response.read().decode(charset)
            logging.info(f"SOAP 응답 상태 코드: {raw_response.status}")
            logging.info(f"SOAP 응답 내용: {response}")
        except Exception as e:
            logging.error(f"HTTP request failed: {e}")
            response = None

        if connection:
            connection.close()
        return response
    
    