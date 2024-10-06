import json
import logging
import jpype
import os

# path = f"{os.environ['PYTHONPATH']}/jars/crypto.jar"
#  path = /jars/crypto.jar"

print(os.environ['PYTHONPATH'])
path = f"{os.environ['PYTHONPATH']}/jars/crypto.jar"
path2 = f"{os.environ['PYTHONPATH']}/jars"
jpype.startJVM(classpath=path)
print("!!!!!!!!!!!",path)
print("asfdasdf",path2)
print("@@@@@@@@", os.listdir(path))
print("@@@@@@@@############", os.listdir(path2))
jpkg = jpype.JPackage('com.indigo.util')
EncryptionUtils = jpkg.EncryptionUtils()

class EhojoUtil:

    def set_url(dtst_cd, params_dict, repeat_num, page_index, interface_id, encrypt_key_ehojo, data_crtr_pnttm):
        """
        dtst_cd별 url 설정
        params: dtst_cd, params_dict, repeat_num, page_index, interface_id, encrypt_key_ehojo
        return: json_data
        """
        # data_header 설정
        data_header = {
            "ifId": interface_id
            , "tranId": str(EncryptionUtils.makeTxId(interface_id))
            , "transfGramNo": ""
            , "trnmtInstCd": "GOS"  # 송신 기관 코드
            , "rcptnInstCd": "MOI"  # 수신 기관 코드
            , "trnmtInstSysCd": "EHJ"  # 송신 기관 시스템 코드
            , "rcptnInstSysCd": "EHJ"  # 수신 기관 시스템 코드
        }
        # data_body 설정
        data_body = {
            "pageSize" : "3000"
            , "curPage" : str(page_index)
        }
        
        if dtst_cd in {"data690", "data691"}:  # 조기집행내역, 집행실적세부사업별통계목별집계
            data_body["lastMdfcnBgngYmd"] = data_crtr_pnttm
        
        #  data_body 조회 조건 필수 값 설정
        if params_dict != {}:  # 사업, 세부사업, 부서코드, 부서코드이력, 세출통계목코드, 회계구분코드 제외
            params = params_dict.get("params")
            data_body["fyr"] = str(params)

        data_body = str(data_body).replace("'","\"")  # {"curPage":"1","pageSize":"10"} 형식으로 맞춤
        logging.info(f"요청 header::: {data_header}, body::: {data_body}")

        # body 암호화 (외부기관에서 요청 시 필요)
        encrypted_body = EncryptionUtils.encryptStringAria(str(data_body), encrypt_key_ehojo)
              
        # JSON 요청 메시지 설정
        data = {
            "header": data_header,
            "body": str(encrypted_body)
        }
        json_data = json.dumps(data).encode("utf-8")
        # logging.info(f"요청 전문::: {json_data}")
        return json_data
    
    def decrypt_body(json_data, encrypt_key_ehojo):
        """
        body 복호화 (외부기관에서 요청 시 필요)
        params: json_data (암호화된 body), encrypt_key_ehojo
        return: json_data (복호화된 body)
        """
        encrypted_body = json_data["body"]
        decrypted_body = json.loads(str(EncryptionUtils.decryptStringAria(encrypted_body, encrypt_key_ehojo)))
        json_data["body"] = decrypted_body
        # logging.info(f"요청 결과 전문::: {json_data}")
        return json_data