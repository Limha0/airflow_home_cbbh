from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class ThDataClctCallFailrHistLog(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    __tablename__ = "th_data_clct_contact_fail_hstry_log"

    sn = Column(Integer, primary_key=True)
    clct_log_sn = Column(Integer)
    dtst_cd = Column(String)
    clct_fail_url = Column(String)
    clct_pgng_no = Column(Integer)
    stts_cd = Column(String)
    stts_msg = Column(String)
    crt_dt = Column(DateTime)
    estn_field_one = Column(String)
    estn_field_two = Column(String)
    estn_field_three = Column(String)
    estn_field_four = Column(String)
    estn_field_five = Column(String)