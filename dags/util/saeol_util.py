import http.client
import random
import socket
import ssl

from urllib.parse import urlparse
from datetime import datetime

class SaeolUtil:
    def get_msg_key():
        """
        메세지 식별키 생성
        return: msg_key
        """
        current_time = datetime.now()
        msg_key = f"{current_time.strftime('%Y%m%d%H%M%S%f')[:-3]}{str(random.random())[2:10]}"
        return msg_key
    
    def make_message(dtst_cd, query_id, params_dict, repeat_num, page_no):
        """
        dtst_cd별 data_list 및 message 생성
        params: dtst_cd, params_dict, repeat_num, page_no
        return: message, message_body
        """
        if params_dict != {}:
            params = params_dict.get("params")
            param_list = params_dict.get("param_list")

        if dtst_cd == "data763":  # 부서정보
            data_key_list = ["ROW_NUM"]
            data_value_map = {"ROW_NUM": page_no}
        if dtst_cd == "data762":  # 직원정보
            data_key_list = ["DEP_CODE"]
            data_value_map = {"DEP_CODE": param_list[repeat_num - 1]}
        if dtst_cd == "data677":  # 새올_민원통계
            data_key_list = ["TAKE_YMD", "REAL_DEAL_YMD", "START_ROW_NO", "END_ROW_NO"]
            data_value_map = {"TAKE_YMD": params, "REAL_DEAL_YMD": params, "START_ROW_NO": page_no, "END_ROW_NO": page_no + 199}

        data_list = ''.join(f"<dataList><data>{data_value_map[key]}</data></dataList>" for key in data_key_list)
        message_body = f"""<query_id>{query_id}</query_id>{data_list}"""
        message = f"""
            <message>
                <body>
                    {message_body}
                </body>
            </message>
        """
        return message, message_body

    def make_req_soap(if_id, src_org_cd, tgt_org_cd, msg_key, message):
        """
        soap 메세지 요청 작성
        params: if_id, src_org_cd, tgt_org_cd, msg_key, message
        return: soap_body
        """
        soap_body = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
                <soapenv:Header/>
                <soapenv:Body>
                    <DOCUMENT>
                        <IFID>{if_id}</IFID>
                        <SRCORGCD>{src_org_cd}</SRCORGCD>
                        <TGTORGCD>{tgt_org_cd}</TGTORGCD>
                        <RESULTCODE>000</RESULTCODE>
                        <MSGKEY>{msg_key}</MSGKEY>
                        <DATA>{message}</DATA>
                    </DOCUMENT>
                </soapenv:Body>
            </soapenv:Envelope>
        """
        return soap_body
    
    def send_http_request(addr, req_soap, charset, if_id):
        """
        http 요청
        params: addr, req_soap, charset, if_id
        return: response
        """
        connection = None
        response = None
        url_parts = urlparse(addr)
        connection = http.client.HTTPConnection(url_parts.netloc, timeout=3600)
        headers = {
            "Content-Type": f"text/xml; charset={charset}",
            "Accept": "application/soap+xml, application/dime, multipart/related, text/*",
            "SOAPAction": if_id,
        }

        connection.request("POST", url_parts.path, req_soap.encode(charset), headers)
        raw_response = connection.getresponse()
        response = raw_response.read().decode(charset)

        if connection:
            connection.close()
        return response