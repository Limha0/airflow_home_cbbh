import os
import logging
import pandas as pd
import zipfile

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Protocol.KDF import PBKDF2

class FileUtil:        
    def check_csv_length(link_file_sprtr, full_file_name):
        """
        csv 파일의 row 개수 확인
        params: full_file_name
        return: row_count
        """
        row_count = 0
        if os.path.exists(full_file_name):
            df = pd.read_csv(full_file_name, sep= link_file_sprtr, low_memory = False)
            row_count = len(df)
        return row_count
    
    
    def zip_file(full_file_path, pvdr_site_nm, link_file_extn, pvdr_sou_data_pvsn_stle):
        """
        파일 zip 압축
        params: full_file_path, pvdr_site_nm, link_file_extn, pvdr_sou_data_pvsn_stle
        """
        # #국가통계포털 수집 전 20240903
        # try:
        #     os.chdir(full_file_path)
        #     full_zip_path = FileUtil.set_full_zip_path(full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle)
        #     with zipfile.ZipFile(full_zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
        #         for root, dirs, files in os.walk(full_file_path):
        #             for file in files:
        #                 if not file.endswith('.zip'):
        #                     file_path = os.path.join(root, file)
        #                     arcname = os.path.relpath(file_path, full_file_path)
        #                     zip_file.write(file_path, arcname)
        # except Exception as e:
        #     logging.info(f"zip_file Exception::: {e}")
        #     raise e
        # logging.info(f"zip_file full_zip_path::: {full_zip_path}")
        try:
            os.chdir(full_file_path)
            full_zip_path = FileUtil.set_full_zip_path(full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle)
            zip_file = zipfile.ZipFile(full_zip_path, "w")  # "w": write 모드, 존재 시 덮어쓰기
            for file in os.listdir(full_file_path):
                file_path = os.path.join(full_file_path, file)
                if (file.endswith(link_file_extn) or file.endswith(pvdr_sou_data_pvsn_stle) or (link_file_extn == 'xls' and file.endswith('xls_sample.csv'))) and not file.endswith('.zip'):  # csv, xml, json, TXT # 국가통계포털 - db인 경우: csv, json / download인 경우: xls, xls_sample.csv
                    zip_file.write(file, compress_type = zipfile.ZIP_DEFLATED)
            zip_file.close()
        except Exception as e:
            logging.info(f"zip_file Exception::: {e}")
            raise e
        logging.info(f"zip_file full_zip_path::: {full_zip_path}")

    def set_full_zip_path(full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle):
        """
        full_zip_path 설정
        params: full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle
        return full_zip_path
        """
        full_zip_path = full_file_path + pvdr_site_nm
        if pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "json":
            full_zip_path += "_db"
        elif pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "xls":
            full_zip_path += "_download"
        full_zip_path += ".zip"
        return full_zip_path
    
    def encrypt_file(full_file_path, pvdr_site_nm, encrypt_key, pvdr_sou_data_pvsn_stle):
        """
        zip 파일 AES-256 암호화
        params: full_file_path, pvdr_site_nm, encrypt_key, pvdr_sou_data_pvsn_stle
        return: full_file_name 암호화된 zip 파일
        """
        try:
            os.chdir(full_file_path)
            if pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "json":
                full_file_name = pvdr_site_nm + "_db_enc.zip"
            elif pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "xls":
                full_file_name = pvdr_site_nm + "_download_enc.zip"
            else:
                full_file_name = pvdr_site_nm + "_enc.zip"

            # 암호화 zip 파일 존재 시 삭제
            # if (os.path.isfile(full_file_path + full_file_name)):
            #     os.remove(full_file_path + full_file_name)

            salt = get_random_bytes(16)
            key = PBKDF2(encrypt_key, salt, dkLen=32, count=100000)
            cipher = AES.new(key, AES.MODE_GCM)
            for file in os.listdir(full_file_path):
                if ((pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "json" and file.endswith("db.zip")) or (pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "xls" and file.endswith("download.zip"))
                    or (pvdr_site_nm != "국가통계포털" and file.endswith("zip"))) and not file.endswith("enc.zip") and not file.startswith("경기도BMS시스템_SHP") and not file.startswith("YANGJU_SHAPE"):
                    with open(file, 'rb') as f:
                        plaintext = f.read()
                    ciphertext, tag = cipher.encrypt_and_digest(plaintext)
                    
                    with open(full_file_name, 'wb') as f:
                        [f.write(x) for x in (salt, cipher.nonce, tag, ciphertext)]
                    
                    # zip 파일 삭제
                    # os.remove(file)
        except Exception as e:
            logging.info(f"encrypt_file Exception::: {e}")
            raise e
        logging.info(f"encrypt_file full_file_name::: {full_file_name}")
        return full_file_name

    def decrypt_file(full_file_path, pvdr_site_nm, encrypt_key, pvdr_sou_data_pvsn_stle):
        """
        zip 파일 AES-256 복호화
        params: full_file_path, pvdr_site_nm, encrypt_key
        """
        full_file_name = pvdr_site_nm
        try:
            os.chdir(full_file_path)
            enc_zip_file_name = "_enc.zip"
            if pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "json":
                full_file_name += "_db"
                enc_zip_file_name = "db_enc.zip"
            elif pvdr_site_nm == "국가통계포털" and pvdr_sou_data_pvsn_stle == "xls":
                full_file_name += "_download"
                enc_zip_file_name = "download_enc.zip"
            full_file_name += ".zip"

            for file in os.listdir(full_file_path):
                if file.endswith(enc_zip_file_name):
                    with open(file, 'rb') as f:
                        salt, nonce, tag, ciphertext = [f.read(x) for x in (16, 16, 16, -1)]
                    key = PBKDF2(encrypt_key, salt, dkLen=32, count=100000)
                    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
                    plaintext = cipher.decrypt_and_verify(ciphertext, tag)
                    with open(full_file_name, 'wb') as f:
                        f.write(plaintext)
        except Exception as e:
            logging.info(f"decrypt_file Exception::: {e}")
            raise e
        return full_file_name
    
    def unzip_file(full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle):
        """
        파일 zip 압축 해제
        params: full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle
        """
        try:
            full_zip_path = FileUtil.set_full_zip_path(full_file_path, pvdr_site_nm, pvdr_sou_data_pvsn_stle)
            with zipfile.ZipFile(full_zip_path, "r") as zf:
                zf.extractall(full_file_path)
        except Exception as e:
            logging.info(f"unzip_file Exception::: {e}")
            raise e
        logging.info(f"unzip_file full_zip_path::: {full_zip_path}")