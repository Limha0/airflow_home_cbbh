class TcCmmnDtlCd():

    def __setattr__(self, *_):
        raise Exception("Tried to change the value of a constant")

    # step_se_cd
    STEP_CNTN = 'step_cntn'  # 접속단계
    STEP_CLCT = 'step_clct'  # 수집단계
    STEP_FILE_INSD_SEND = 'step_file_insd_send'  # 내부파일전송단계
    STEP_FILE_STRGE_SEND = 'step_file_strge_send'  # 하둡파일전송단계
    STEP_DW_LDADNG = 'step_dw_ldadng'  # 데이터웨어하우스적재단계
    STEP_ADDR_REFINE = 'step_addr_refine'  # 주소정제단계
    STEP_DTMT_PRCSS = 'step_dtmt_prcss'  # 데이터마트가공단계

    # stts_cd
    STTS_WORK = 'work'  # 작업중
    STTS_COMP = 'comp'  # 완료
    STTS_ERROR = 'error'  # 오류

    # stts_msg
    MSG_CNTN_WORK = '수집대상 조회 시작'
    MSG_CNTN_ERROR = '연계DB접속정보 조회 실패'
    MSG_CNTN_COMP = '수집대상 조회 완료'

    MSG_CLCT_WORK = '수집파일경로 생성 시작'
    MSG_CLCT_ERROR_PATH = '수집파일경로 생성 실패'
    MSG_CLCT_COMP = 'URL호출 및 CSV생성 성공'
    MSG_CLCT_COMP_DB = 'CSV생성 성공'	
    MSG_CLCT_COMP_NO_DATA = '원천데이터 없음'
    MSG_CLCT_ERROR_CALL = 'URL호출 및 CSV생성 실패'
    MSG_CLCT_ERROR_DB = 'CSV생성 실패'
    MSG_CLCT_ERROR_MATCH = '파일명, 행정분야명 불일치'

    MSG_FILE_INSD_SEND_WORK = '파일압축 및 암호화 시작'
    MSG_FILE_INSD_SEND_ERROR_FILE = '파일압축 및 암호화 실패'
    MSG_FILE_INSD_SEND_COMP_EXT = '암호화 파일 내부서버 전송 성공'
    MSG_FILE_INSD_SEND_COMP_INT = '내부서버 적재 성공'
    MSG_FILE_INSD_SEND_ERROR_TRANS_EXT = '암호화 파일 내부서버 전송 실패'

    MSG_FILE_STRGE_SEND_WORK = '복호화 대상 조회 시작'
    MSG_FILE_STRGE_SEND_WORK_UNZIP = '복호화 및 파일압축 해제 성공'
    MSG_FILE_STRGE_SEND_ERROR_UNZIP = '복호화 및 파일압축 해제 실패'
    MSG_FILE_STRGE_SEND_WORK_CHECK = '최종경로에 파일존재 확인 성공'
    MSG_FILE_STRGE_SEND_ERROR_CHECK = '최종경로에 파일 없음'
    # MSG_FILE_STRGE_SEND_COMP = '스토리지 전송 성공'
    MSG_FILE_STRGE_SEND_COMP = '하둡 전송 성공'
    # MSG_FILE_STRGE_SEND_ERROR_MOVE = '스토리지 전송 실패'
    MSG_FILE_STRGE_SEND_ERROR_MOVE = '하둡 전송 실패'

    MSG_DW_LDADNG_WORK = 'DW 적재 대상 조회 시작'
    MSG_DW_LDADNG_ERROR_FILE = 'DB서버에 파일 전송 실패'
    MSG_DW_LDADNG_ERROR_DATA = '컬럼명 및 데이터타입 획득 실패'
    MSG_DW_LDADNG_COMP = 'DW 적재 성공'
    MSG_DW_LDADNG_ERROR_DW = 'DW 적재 실패'