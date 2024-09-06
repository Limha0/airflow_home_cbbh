from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class ThDataClctMastrLog(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    __tablename__ = "th_data_clct_mastr_log"

    clct_log_sn = Column(Integer, primary_key=True)
    dtst_cd = Column(String)
    clct_ymd = Column(String)
    clct_data_nm = Column(String)
    # pbadms_fld_cd = Column(String)
    # gg_ctgry_cd = Column(String)
    data_crtr_pnttm = Column(String)
    reclect_flfmt_nmtm = Column(Integer)
    step_se_cd = Column(String)
    stts_cd = Column(String)
    stts_dt = Column(DateTime)
    stts_msg = Column(String)
    crt_dt = Column(DateTime)
    dw_rcrd_cnt = Column(Integer)
    creatr_id = Column(String)
    creatr_nm = Column(String)
    creatr_dept_nm = Column(String)
    estn_field_one = Column(String)
    estn_field_two = Column(String)
    estn_field_three = Column(String)
    link_file_sprtr = Column(String)