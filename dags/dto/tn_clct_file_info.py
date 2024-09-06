from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class TnClctFileInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    __tablename__ = "tn_clct_file_info"
    
    sn = Column(Integer, primary_key=True)
    clct_log_sn = Column(Integer)
    dtst_cd = Column(String)
    clct_ymd = Column(String)
    clct_data_nm = Column(String)
    data_crtr_pnttm = Column(String)
    insd_flpth = Column(String)
    insd_file_nm = Column(String)
    insd_file_extn = Column(String)
    insd_file_size = Column(Integer)
    dwnld_nocs = Column(Integer)
    inq_nocs = Column(Integer)
    crt_dt = Column(DateTime)
    use_yn = Column(String)