from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class ThDataClctSttsHistLog(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    __tablename__ = "th_data_clct_stts_hstry_log"

    clct_log_sn = Column(Integer, primary_key=True)
    clct_stts_sn = Column(Integer, primary_key=True, autoincrement=True)
    reclect_yn = Column(String)
    step_se_cd = Column(String)
    stts_cd = Column(String)
    stts_dt = Column(DateTime)
    stts_msg = Column(String)