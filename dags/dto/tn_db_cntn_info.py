from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String

Base = declarative_base()

class TnDBCntnInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    __tablename__ = "tn_db_cntn_info"

    link_db_id = Column(String, primary_key=True)
    pvdr_sys_nm = Column(String)
    db_knd_cd = Column(String)
    sid = Column(String)
    ip = Column(String)
    port = Column(String)
    user_id = Column(String)
    pswd = Column(String)
    driver_class_nm = Column(String)