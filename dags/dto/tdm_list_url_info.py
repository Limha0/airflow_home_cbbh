from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class TdmListUrlInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    __tablename__ = "tdw_etc_dtst_list_api_contact_rslt_cbbh"

    clct_sn = Column(Integer, primary_key=True)
    category_cd = Column(String)
    category_nm = Column(String)
    collection_method = Column(String)
    created_at = Column(String)
    desc = Column(String)
    download_cnt = Column(String)
    ext = Column(String)
    id = Column(String)
    is_deleted = Column(String)
    is_requested_data = Column(String)
    keywords = Column(String)
    list_type = Column(String)
    new_category_cd = Column(String)
    new_category_nm = Column(String)
    org_cd = Column(String)
    org_nm = Column(String)
    ownership_grounds = Column(String)
    page_url = Column(String)
    providing_scope = Column(String)
    register_status = Column(String)
    title = Column(String)
    updated_at = Column(String)
    view_cnt = Column(String)
    data_crtr_pnttm = Column(String, primary_key=True)
    clct_pnttm = Column(String)
    clct_log_sn = Column(Integer)
    page_no = Column(Integer)