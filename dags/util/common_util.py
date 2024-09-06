import csv
import logging
import os
from datetime import timedelta, timezone, datetime as dt
from pendulum import now

# from tdm_list_url_info import TdmListUrlInfo
from dto.tn_data_bsc_info import TnDataBscInfo
from dto.th_data_clct_mastr_log import ThDataClctMastrLog
from dto.th_data_clct_stts_hstry_log import ThDataClctSttsHistLog
from dto.tn_clct_file_info import TnClctFileInfo
from dto.tc_com_dtl_cd import TcCmmnDtlCd as CONST

class CommonUtil:
    def set_data_crtr_pnttm(link_clct_cycle_cd, data_interval_start):
        """
        link_clct_cycle_cd 에 따른 data_crtr_pnttm 설정
        params: link_clct_cycle_cd (데이터_수집_주기_코드), data_interval_start (처리 데이터의 날짜)
        return: data_crtr_pnttm (데이터 기준 시점)
        """
        data_crtr_pnttm = { 'year' : data_interval_start.strftime("%Y"),
                        'quarter' : data_interval_start.strftime("%Y%m"),
                        'month' : data_interval_start.strftime("%Y%m"),
                        '3day' : data_interval_start.add(days=2).strftime("%Y%m%d"),
                        'day' : data_interval_start.strftime("%Y%m%d"),
                        'hour' : data_interval_start.strftime("%Y%m%d"),
                        '5min' : data_interval_start.strftime("%Y%m%d") }.get(link_clct_cycle_cd)
        return data_crtr_pnttm
    
    def insert_collect_data_info(select_bsc_info_stmt, session, data_interval_start, data_interval_end, kwargs):
        """
        tn_data_bsc_info 테이블에서 수집 대상 기본 정보 조회 후 th_data_clct_mastr_log, th_data_clct_stts_hstry_log 테이블에 입력
        params: select_bsc_info_stmt, session, data_interval_start, data_interval_end
        return: collect_data_list
        """
        collect_data_list = []
        try:
            with session.begin() as conn:
                for dict_row in conn.execute(select_bsc_info_stmt).all():
                    tn_data_bsc_info = TnDataBscInfo(**dict_row)
                    dtst_cd = tn_data_bsc_info.dtst_cd.lower()

                    if dtst_cd == 'data650':  # 한국천문연구원_특일_정보
                        data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start.add(years=1))
                    else:
                        data_crtr_pnttm = CommonUtil.set_data_crtr_pnttm(tn_data_bsc_info.link_clct_cycle_cd, data_interval_start)

                    file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_") + "_" + data_crtr_pnttm
                    
                    # 기상청_단기예보, 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회, 새올행정주민요약DB 로그 존재 확인
                    if dtst_cd in {"data852", "data4", "data799", "data31", 'data855', "data793", "data795", "data792", "data794"}:
                        th_data_clct_mastr_log = CommonUtil.get_exist_log(conn, data_crtr_pnttm, dtst_cd)

                    # 기상청_단기예보, 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회, 새올행정주민요약DB의 경우 로그 존재하지 않을 때
                    if (dtst_cd in {"data852", 'data4', 'data799', 'data31', 'data855', "data793", "data795", "data792", "data794"} and th_data_clct_mastr_log == None) \
                        or dtst_cd not in {"data852", 'data4', 'data799', 'data31', 'data855', "data793", "data795", "data792", "data794"}:
                        # th_data_clct_mastr_log 테이블에 insert
                        th_data_clct_mastr_log = ThDataClctMastrLog()
                        th_data_clct_mastr_log.dtst_cd = dtst_cd
                        th_data_clct_mastr_log.clct_ymd = data_interval_end.strftime("%Y%m%d")
                        th_data_clct_mastr_log.clct_data_nm = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                        # th_data_clct_mastr_log.gg_ctgry_cd = tn_data_bsc_info.pbadms_fld_lclsf_cd  # pbadms_fld_lclsf_cd( 행정분야대분류코드로 경산코드 분류 )
                        # th_data_clct_mastr_log.pbadms_fld_cd = tn_data_bsc_info.pbadms_fld_cd
                        th_data_clct_mastr_log.data_crtr_pnttm = data_crtr_pnttm
                        th_data_clct_mastr_log.reclect_flfmt_nmtm = 0
                        th_data_clct_mastr_log.step_se_cd = CONST.STEP_CNTN
                        th_data_clct_mastr_log.stts_cd = CONST.STTS_WORK
                        th_data_clct_mastr_log.stts_dt = now(tz="UTC")
                        th_data_clct_mastr_log.stts_msg = CONST.MSG_CNTN_WORK
                        th_data_clct_mastr_log.crt_dt = now(tz="UTC")
                        th_data_clct_mastr_log.link_file_sprtr = tn_data_bsc_info.link_file_sprtr
                        conn.add(th_data_clct_mastr_log)
                        conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)
                    # # 불필요한 키 제거
                    # th_data_clct_mastr_log = ThDataClctMastrLog()
                    # th_data_clct_mastr_log.dtst_cd = dtst_cd
                    # th_data_clct_mastr_log.clct_ymd = data_interval_end.strftime("%Y%m%d")
                    # th_data_clct_mastr_log.clct_data_nm = tn_data_bsc_info.dtst_nm.replace(" ", "_")
                    # # th_data_clct_mastr_log.pbadms_fld_cd = tn_data_bsc_info.pbadms_fld_cd
                    # th_data_clct_mastr_log.data_crtr_pnttm = data_crtr_pnttm
                    # th_data_clct_mastr_log.reclect_flfmt_nmtm = 0
                    # th_data_clct_mastr_log.step_se_cd = CONST.STEP_CNTN
                    # th_data_clct_mastr_log.stts_cd = CONST.STTS_WORK
                    # th_data_clct_mastr_log.stts_dt = now(tz="UTC")
                    # th_data_clct_mastr_log.stts_msg = CONST.MSG_CNTN_WORK
                    # th_data_clct_mastr_log.crt_dt = now(tz="UTC")
                    # th_data_clct_mastr_log.link_file_sprtr = tn_data_bsc_info.link_file_sprtr
                    # conn.add(th_data_clct_mastr_log)
                    # conn.get(ThDataClctMastrLog, th_data_clct_mastr_log.clct_log_sn)

                    # 기상청_단기예보, 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회, 새올행정주민요약DB - th_data_clct_stts_hstry_log 입력위한 th_data_clct_mastr_log 설정
                    if dtst_cd in {"data852", 'data4', 'data799', 'data31', 'data855', "data793", "data795", "data792", "data794"}:
                        th_data_clct_mastr_log.step_se_cd = CONST.STEP_CNTN
                        th_data_clct_mastr_log.stts_cd = CONST.STTS_WORK
                        th_data_clct_mastr_log.stts_msg = CONST.MSG_CNTN_WORK

                    # th_data_clct_stts_hstry_log 테이블에 insert
                    CommonUtil.insert_history_log(conn, th_data_clct_mastr_log, "n")

                    # th_data_clct_stts_hstry_log 수집파일정보 set
                    tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                    # 수집로그파일 경로 생성
                    log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, data_interval_end, kwargs)

                    collect_data_list.append({
                                              "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                                            , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                            , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                            , "log_full_file_path" : log_full_file_path
                                            })
            # 수집로그파일 생성
            for log_data_list in collect_data_list:
                CommonUtil.create_log_file(log_data_list['log_full_file_path'], log_data_list['tn_clct_file_info'], session)
        except Exception as e:
            logging.info(f"insert_collect_data_info Exception::: {e}")
            raise e
        return collect_data_list
    
    
    def insert_history_log(conn, th_data_clct_mastr_log, reclect_yn):
        """ 
        th_data_clct_stts_hstry_log 테이블에 로그 insert
        params: conn, th_data_clct_mastr_log, reclect_yn
        """
        th_data_clct_stts_hstry_log = ThDataClctSttsHistLog()
        th_data_clct_stts_hstry_log.clct_log_sn = th_data_clct_mastr_log.clct_log_sn
        th_data_clct_stts_hstry_log.reclect_yn = reclect_yn
        th_data_clct_stts_hstry_log.step_se_cd = th_data_clct_mastr_log.step_se_cd 
        th_data_clct_stts_hstry_log.stts_cd = th_data_clct_mastr_log.stts_cd
        th_data_clct_stts_hstry_log.stts_dt = th_data_clct_mastr_log.stts_dt
        th_data_clct_stts_hstry_log.stts_msg = th_data_clct_mastr_log.stts_msg
        conn.add(th_data_clct_stts_hstry_log)


    def update_file_info_table(session, th_data_clct_mastr_log, tn_clct_file_info, file_name, file_path, file_extn, file_size):
        """
        tn_clct_file_info 테이블 insert (존재 시 업데이트)
        params: session, th_data_clct_mastr_log, tn_clct_file_info, file_name, file_path, file_size
        """
        with session.begin() as conn:
            select_stmt = f"""
                            SELECT *
                            FROM tn_clct_file_info
                            WHERE clct_log_sn = {th_data_clct_mastr_log.clct_log_sn}
                            """
            dict_row = conn.execute(select_stmt).first()
            if dict_row == None:  # insert
                tn_clct_file_info = CommonUtil.set_file_info(tn_clct_file_info, th_data_clct_mastr_log, file_name, file_path, file_extn, file_size, dict_row)
                conn.add(tn_clct_file_info) 
            else:  # 존재 시 update
                tn_clct_file_info = conn.get(TnClctFileInfo, dict_row.sn)
                CommonUtil.set_file_info(tn_clct_file_info, th_data_clct_mastr_log, file_name, file_path, file_extn, file_size, dict_row)
                conn.merge(tn_clct_file_info)
                logging.info(f"update_file_info_table update::: {th_data_clct_mastr_log.clct_log_sn}")


    def set_file_info(tn_clct_file_info, th_data_clct_mastr_log, file_name, file_path, file_extn, file_size, dict_row):
        """
        set tn_clct_file_info
        params: tn_clct_file_info, th_data_clct_mastr_log, file_name, file_path, file_size, dict_row (select 결과)
        return: tn_clct_file_info
        """
        tn_clct_file_info.clct_log_sn = th_data_clct_mastr_log.clct_log_sn
        tn_clct_file_info.dtst_cd = th_data_clct_mastr_log.dtst_cd
        tn_clct_file_info.clct_ymd = th_data_clct_mastr_log.clct_ymd
        tn_clct_file_info.clct_data_nm = th_data_clct_mastr_log.clct_data_nm
        tn_clct_file_info.data_crtr_pnttm = th_data_clct_mastr_log.data_crtr_pnttm
        tn_clct_file_info.insd_flpth = file_path
        tn_clct_file_info.insd_file_nm = file_name
        tn_clct_file_info.insd_file_extn = file_extn
        tn_clct_file_info.insd_file_size = file_size
        if tn_clct_file_info.dwnld_nocs == None:
            tn_clct_file_info.dwnld_nocs = 0
        if tn_clct_file_info.inq_nocs == None:
            tn_clct_file_info.inq_nocs = 0
        tn_clct_file_info.use_yn = "y"
        
        if dict_row == None:
            tn_clct_file_info.crt_dt = now(tz="UTC")
        return tn_clct_file_info
    
    def create_log_file_directory(tn_data_bsc_info, data_interval_end, kwargs):
        """
        수집로그파일 경로 생성
        params: tn_data_bsc_info, data_interval_end
        return: log_full_file_path
        """
        collect_log_file_path = kwargs['var']['value'].collect_log_file_path
        # 파일 경로 설정
        file_path, log_full_file_path = CommonUtil.set_file_path(collect_log_file_path, data_interval_end, tn_data_bsc_info)
        try:
            # 수집로그파일 폴더 경로 생성
            os.makedirs(log_full_file_path, exist_ok=True)
        except OSError as e:
            logging.info(f"create_log_file_directory OSError::: {e}")
        return log_full_file_path

    def create_log_file(log_full_file_path, tn_clct_file_info, session):
        """
        수집로그파일 생성
        params: log_full_file_path, tn_clct_file_info, session
        """
        try:
            os.chdir(log_full_file_path)
            insd_file_nm = ""
            insd_file_size = ""
            clct_log_sn = f" (clct_log_sn={tn_clct_file_info['clct_log_sn']}) "
            if tn_clct_file_info['dtst_cd'] == 'data648':  # 지방행정인허가
                log_file_name = tn_clct_file_info['clct_data_nm'] + "_" + tn_clct_file_info['data_crtr_pnttm'] + ".csv"
                insd_file_nm = f" {tn_clct_file_info['insd_file_nm']:<50} "
            else:
                log_file_name = tn_clct_file_info['insd_file_nm'] + ".csv"
            mode = "w"  # 파일 쓰기 모드
            with session.begin() as conn:
                th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, tn_clct_file_info['clct_log_sn'])
                log_message = th_data_clct_mastr_log.stts_msg
                if tn_clct_file_info['insd_file_size'] != None and th_data_clct_mastr_log.step_se_cd == CONST.STEP_CLCT:
                    insd_file_size = f" file_size={tn_clct_file_info['insd_file_size']}(byte)"
                if os.path.exists(log_full_file_path + log_file_name):
                    mode = "a"
            data_list = [f"[{now().format('YYYY-MM-DD HH:mm:ss.SSSSSS')}]{clct_log_sn}{insd_file_nm} {log_message:<20}{insd_file_size:<20}"]
            with open(log_file_name, mode) as file:
                write = csv.writer(file, delimiter = "|")
                write.writerow(data_list)
        except Exception as e:
            logging.info(f"create_log_file Exception::: {e}")
            

    def create_directory(collect_data_list, session, data_interval_end, root_collect_file_path, reclect_yn):
        """
        수집 파일 경로 생성
        params: collect_data_list (dict),session, data_interval_end, root_collect_file_path, reclect_yn
        return: file_path 파일 경로
        """
        temp_list = []
        if isinstance(collect_data_list, list):  # list 인 경우
            temp_list.extend(collect_data_list)
        else:  # dict 인 경우
            temp_list.append(collect_data_list)
        for collect_data_dict in temp_list:
            tn_data_bsc_info = TnDataBscInfo(**collect_data_dict['tn_data_bsc_info'])
            dtst_cd = tn_data_bsc_info.dtst_cd.lower()
            link_file_crt_yn = tn_data_bsc_info.link_file_crt_yn.lower()  # csv 파일 생성 여부

            tn_clct_file_info = TnClctFileInfo(**collect_data_dict['tn_clct_file_info'])
            th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_dict['th_data_clct_mastr_log'])
            log_full_file_path = collect_data_dict['log_full_file_path']

            # 수집파일경로 생성 시작 로그 update
            CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CNTN, CONST.STTS_COMP, CONST.MSG_CNTN_COMP, reclect_yn)
            # 수집파일경로생성 시작 로그 update
            if dtst_cd not in {'data852', 'data4', 'data31', 'data855', 'data799'}:  # 기상청_단기예보_시간, 5분_소통정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회, 센서측정정보 제외
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_WORK, CONST.MSG_CLCT_WORK, reclect_yn)

            file_path, full_file_path = CommonUtil.set_file_path(root_collect_file_path, data_interval_end, tn_data_bsc_info)
            # 파일 경로 설정
            # if dtst_cd == 'data1':  # 부서정보, 직원정보
            #     # file_path = full_file_path = root_collect_file_path
            #     file_path, full_file_path = CommonUtil.set_file_path(root_collect_file_path, data_interval_end, tn_data_bsc_info)
        try:
            if link_file_crt_yn == 'y' or tn_data_bsc_info.pvdr_site_cd == 'ps00010':  # 국가통계포털 download
                # 수집 폴더 경로 생성
                os.makedirs(full_file_path, exist_ok=True)
        except OSError as e:
            for collect_data_dict in temp_list:
                th_data_clct_mastr_log = ThDataClctMastrLog(**collect_data_dict['th_data_clct_mastr_log'])
                CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CLCT, CONST.STTS_ERROR, CONST.MSG_CLCT_ERROR_PATH, reclect_yn)
            logging.error(f"create_directory OSError::: {e}")
            raise e
        logging.info(f"create_directory full_file_path::: {full_file_path}")
        return file_path
    
    def update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, step_se_cd, status_code, message, reclect_yn):
        """
        th_data_clct_mastr_log 테이블에 로그 update 및 
        th_data_clct_stts_hstry_log 테이블에 로그 insert
        params: session, tn_data_bsc_info, th_data_clct_mastr_log, step_se_cd, status_code, message, reclect_yn
        """
        try:
            with session.begin() as conn:
                # th_data_clct_mastr_log
                th_data_clct_mastr_log.step_se_cd = step_se_cd
                th_data_clct_mastr_log.stts_cd = status_code
                th_data_clct_mastr_log.stts_dt = now(tz="UTC")
                th_data_clct_mastr_log.stts_msg = message
                th_data_clct_mastr_log.crt_dt = th_data_clct_mastr_log.crt_dt.astimezone(timezone.utc)
                conn.merge(th_data_clct_mastr_log)

                # th_data_clct_stts_hstry_log
                CommonUtil.insert_history_log(conn, th_data_clct_mastr_log, reclect_yn)

            # 수집로그파일 생성
            CommonUtil.create_log_file(log_full_file_path, tn_clct_file_info.as_dict(), session)

        except Exception as e:
            logging.info(f"update_log_table Exception::: {e}")
            raise e
        

    def set_file_path(root_file_path, data_interval_end, tn_data_bsc_info):
        """
        파일 경로 설정
        params: root_file_path (상위 폴더 경로), data_interval_end, tn_data_bsc_info
        return: file_path, full_file_path
        """
        # if tn_data_bsc_info.dtst_cd == 'data891':  # 노선별OD데이터 예외
        #     file_path = tn_data_bsc_info.pvdr_site_nm + "/" + data_interval_end.strftime("%Y") + "/" + data_interval_end.strftime("%m") + "/"
        # else:
        file_name = tn_data_bsc_info.dtst_nm.replace(" ", "_")  # file_name을 문자열로 정의
        file_path = data_interval_end.strftime("%Y") + "년/" + file_name + "/"
        file_path += { 'year' : data_interval_end.strftime("%Y"),
                        'quarter' : data_interval_end.strftime("%Y%m"),
                        'month' : data_interval_end.strftime("%Y%m"),
                        '3day' : data_interval_end.strftime("%Y%m%d"),
                        'day' : data_interval_end.strftime("%Y%m%d"),
                        'hour' : data_interval_end.strftime("%Y%m%d"),
                        '5min' : data_interval_end.strftime("%Y%m%d") }.get(tn_data_bsc_info.link_clct_cycle_cd) + "/"
        full_file_path = root_file_path + file_path
        return file_path, full_file_path
    
    def set_fail_info(session, select_log_info_stmt, kwargs):
        """
        재수집 대상 기본 정보 조회, 수집파일정보 및 수집로그파일 경로 set
        params: session, select_log_info_stmt
        return: collect_data_list
        """
        collect_data_list = []
        with session.begin() as conn:
            for dict_row in conn.execute(select_log_info_stmt).all():
                th_data_clct_mastr_log = ThDataClctMastrLog(**dict_row)
                dtst_cd = th_data_clct_mastr_log.dtst_cd
                if dtst_cd not in {'data4', 'data799', 'data31', 'data855'}:  # 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회 제외
                    th_data_clct_mastr_log.reclect_flfmt_nmtm += 1  # 재수집 수행 횟수 증가

                # 재수집 대상 기본 정보 조회
                select_bsc_info_stmt = f'''
                                    SELECT *, (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                                    FROM tn_data_bsc_info
                                    WHERE 1=1
                                        AND LOWER(clct_yn) = 'y'
                                        AND LOWER(link_yn) = 'y'
                                        AND LOWER(dtst_cd) = '{dtst_cd}'
                                    '''
                # if dtst_cd == "data675":  # 신문고
                #     select_bsc_info_stmt = f'''
                #                             SELECT sn, a.dtst_cd, b.dtst_nm, dtst_dc, a.pvdr_site_cd, pvdr_inst_cd, pvdr_dept_nm, pvdr_pvsn_mthd_cd, pvdr_data_se_value_one, pvdr_data_se_value_two
                #                                 , pvdr_updt_cycle_cd, pvdr_sou_data_pvsn_stle, link_ntwk_otsd_insd_se, link_pvdr_url, link_data_clct_url, link_db_id, link_tbl_phys_nm, link_dtst_se_cd, link_geom_utlz_yn, link_file_crt_yn
                #                                 , link_file_merg_yn, link_file_extn, a.link_file_sprtr, link_yn, link_se_cd, clct_yn, link_clct_mthd, link_clct_mthd_dtl_cd, link_clct_cycle_cd, link_clct_job_id
                #                                 , b.pbadms_fld_cd, b.gg_ctgry_cd, nonidntf_prcs_yn, encpt_yn, a.dw_load_yn, dw_load_mthd_cd, b.dw_tbl_phys_nm, addr_refine_yn, dtwrh_utlz_yn, data_rls_se_cd
                #                                 , portal_dwnld_pvsn_yn, a.use_yn, crt_dt, rmrk, rmrk_two, rmrk_three, data_se_col_one, data_se_col_two, rfrnc_phys_tbl_nm, rfrnc_col_nm, crtr_del_col_nm
                #                                 , (SELECT dtl_cd_nm FROM tc_com_dtl_cd WHERE group_cd = 'pvdr_site_cd' AND a.pvdr_site_cd = dtl_cd) AS pvdr_site_nm
                #                             FROM tn_data_bsc_info a, tc_pbadms_fld_mapng b
                #                             WHERE 1=1
                #                             AND a.dtst_cd = b.dtst_cd
                #                             AND a.dtst_cd = 'data675'
                #                             AND b.dtst_nm = '{th_data_clct_mastr_log.clct_data_nm}'
                #                             ORDER BY a.dtst_cd
                #                             '''
                dict_row_info = conn.execute(select_bsc_info_stmt).first()
                tn_data_bsc_info = TnDataBscInfo(**dict_row_info)

                # 수집파일정보 set
                if dtst_cd in {"data762", "data763"}:  # 부서정보, 직원정보 예외
                    file_name = th_data_clct_mastr_log.clct_data_nm
                else:
                    file_name = th_data_clct_mastr_log.clct_data_nm + "_" + th_data_clct_mastr_log.data_crtr_pnttm
                tn_clct_file_info = CommonUtil.set_file_info(TnClctFileInfo(), th_data_clct_mastr_log, file_name, None, tn_data_bsc_info.link_file_extn, None, None)

                # 수집로그파일 경로 set
                log_full_file_path = CommonUtil.create_log_file_directory(tn_data_bsc_info, dt.strptime(th_data_clct_mastr_log.clct_ymd,"%Y%m%d"), kwargs)

                if dtst_cd in {'data4', 'data799', 'data31', 'data855'}:  # 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_FILE_INSD_SEND, CONST.STTS_WORK, "전송대상 조회 시작", "n")
                else:
                    CommonUtil.update_log_table(log_full_file_path, tn_clct_file_info, session, th_data_clct_mastr_log, CONST.STEP_CNTN, CONST.STTS_WORK, CONST.MSG_CNTN_WORK, "y")

                collect_data_list.append({
                                        "tn_data_bsc_info" : tn_data_bsc_info.as_dict()
                                        , "th_data_clct_mastr_log": th_data_clct_mastr_log.as_dict()
                                        , "tn_clct_file_info": tn_clct_file_info.as_dict()
                                        , "log_full_file_path" : log_full_file_path
                                        })
        return collect_data_list
    
    def get_exist_log(conn, data_crtr_pnttm, dtst_cd):
        """
        기상청_단기예보, 5분_소통정보, 센서측정정보, 대기오염정보_측정소별_실시간_측정정보_조회, 실시간_측정정보_조회, 새올행정주민요약DB 로그 존재 확인
        params: conn, data_crtr_pnttm, dtst_cd
        return: th_data_clct_mastr_log
        """
        select_log_info_stmt = f'''
                            SELECT b.*
                            FROM tn_data_bsc_info a , th_data_clct_mastr_log b
                            WHERE 1=1
                            AND a.dtst_cd = b.dtst_cd 
                                AND LOWER(a.link_yn) = 'y'
                                AND data_crtr_pnttm = '{data_crtr_pnttm}'
                                AND a.dtst_cd = '{dtst_cd}'
                            '''
        dict_row = conn.execute(select_log_info_stmt).first()
        if dict_row == None:
            th_data_clct_mastr_log = None
        else:
            th_data_clct_mastr_log = conn.get(ThDataClctMastrLog, dict_row.clct_log_sn)
            th_data_clct_mastr_log.crt_dt = th_data_clct_mastr_log.crt_dt.astimezone(timezone.utc)
        return th_data_clct_mastr_log