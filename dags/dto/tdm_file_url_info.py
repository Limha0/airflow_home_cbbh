from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime

Base = declarative_base()

class TdmFileUrlInfo(Base):

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    __tablename__ = "tdw_etc_file_data_list_api_contact_rslt"

    clct_sn = Column(Integer, primary_key=True)
    core_data_nm = Column(String)
    cost_unit = Column(String)
    created_at = Column(String)
    data_limit = Column(String)
    data_type = Column(String)
    dept_nm = Column(String)
    desc = Column(String)
    download_cnt = Column(String)
    etc = Column(String)
    ext = Column(String)
    id = Column(String)
    is_charged = Column(String)
    is_copyrighted = Column(String)
    is_core_data = Column(String)
    is_deleted = Column(String)
    is_list_deleted = Column(String)
    is_std_data = Column(String)
    is_third_party_copyrighted = Column(String)
    keywords = Column(String)
    list_id = Column(String)
    list_title = Column(String)
    media_cnt = Column(String)
    media_type = Column(String)
    meta_url = Column(String)
    new_category_cd = Column(String)
    new_category_nm = Column(String)
    next_registration_date = Column(String)
    org_cd = Column(String)
    org_nm = Column(String)
    ownership_grounds = Column(String)
    regist_type = Column(String)
    register_status = Column(String)
    share_scope_nm = Column(String)
    title = Column(String)
    update_cycle = Column(String)
    updated_at = Column(String)
    data_crtr_pnttm = Column(String, primary_key=True)
    clct_pnttm = Column(String)
    clct_log_sn = Column(Integer)
    page_no = Column(Integer)