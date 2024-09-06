from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class TdmStandardUrlInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    __tablename__ = "tdw_etc_std_dtst_list_api_contact_rslt"

    clct_sn = Column(Integer, primary_key = True)
    category_cd = Column(String)
    category_nm = Column(String)
    collection_method = Column(String)
    created_at = Column(String)
    dept_nm = Column(String)
    desc = Column(String)
    id = Column(String)
    is_requested_data = Column(String)
    keywords = Column(String)
    list_id = Column(String)
    list_title = Column(String)
    list_type = Column(String)
    new_category_cd = Column(String)
    new_category_nm = Column(String)
    next_registration_date = Column(String)
    org_cd = Column(String)
    org_nm = Column(String)
    ownership_grounds = Column(String)
    providing_scope = Column(String)
    req_cnt = Column(String)
    title = Column(String)
    updated_dt = Column(String)
    data_crtr_pnttm = Column(String, primary_key = True)
    clct_pnttm = Column(String)
    clct_log_sn = Column(Integer)
    page_no = Column(Integer)