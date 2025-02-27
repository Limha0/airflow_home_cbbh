import json
import logging
import jpype
import os

# import configparser

# # ConfigParser 객체 생성
# config = configparser.ConfigParser()

# # properties 파일 읽기
# #config.read('config.properties')
# config.read('/opt/airflow/dags/config.properties') #prod

# # 환경 변수 설정
# os.environ['PYTHONPATH'] = config.get('settings', 'PYTHONPATH')
# os.environ['JAVA_HOME'] = config.get('settings', 'JAVA_HOME')

# # PATH는 기존 값과 합쳐서 설정
# os.environ['PATH'] = f"{config.get('settings', 'PATH')}:{os.environ['PATH']}"

# # JAR 파일 경로 설정
# path = f"{os.environ['PYTHONPATH']}/jars/crypto.jar"

# # JVM 시작 (classpath에 설정한 JAR 경로 추가)
# jpype.startJVM(classpath=path)

# # Java 패키지 및 클래스 로드
# jpkg = jpype.JPackage('com.indigo.util')
# EncryptionUtils = jpkg.EncryptionUtils()


path = f"{os.environ['PYTHONPATH']}/jars/crypto.jar"
jpype.startJVM(classpath=path)
jpkg = jpype.JPackage('com.indigo.util')
EncryptionUtils = jpkg.EncryptionUtils()

# 결과 확인
# logging.info(f"PYTHONPATH: {os.environ['PYTHONPATH']}")
# logging.info(f"JAVA_HOME: {os.environ['JAVA_HOME']}")
# logging.info(f"PATH: {os.environ['PATH']}")
# logging.info(f"JAR 파일 경로: {path}")


class EhojoUtil:

    def set_url(dtst_cd, params_dict, repeat_num, pagd_no, interface_id, encrypt_key_ehojo, data_crtr_pnttm):
        """
        dtst_cd별 url 설정
        params: dtst_cd, params_dict, repeat_num, pagd_no, interface_id, encrypt_key_ehojo
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
            , "curPage" : str(pagd_no)
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
        #logging.info(f"요청 전문::: {json_data}")
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
        #logging.info(f"요청 결과 전문::: {json_data}")
        return json_data
