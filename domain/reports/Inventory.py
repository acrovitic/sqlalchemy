from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, BigInteger
from typing import Any, Union

Base: Union[DeclarativeMeta, Any] = declarative_base()


class Inv(Base):
    __tablename__ = 'inventory'
    __table_args__ = {"schema": "reports"}

    creation_date = Column(DateTime(timezone=False))
    modify_date = Column(DateTime(timezone=False))
    object_id = Column(String, primary_key=True)
    folder_id = Column(String)
    folder_type = Column(String)
    sub_type = Column(String)
    object_name = Column(String)
    uniquekey = Column(String)
    guid = Column(String)
    full_content_size = Column(Float)
    number_of_pages = Column(BigInteger)
    doc_download_status = Column(String)
    doc_validation_status = Column(String)
    doc_validation_date = Column(DateTime(timezone=False))
    doc_download_date = Column(DateTime(timezone=False))
    document_date = Column(DateTime(timezone=False))
    scan_date = Column(DateTime(timezone=False))
    index_date = Column(DateTime(timezone=False))
    load_date = Column(DateTime(timezone=False))
    update_date = Column(DateTime(timezone=False))
    comment_doc_failure = Column(String)


    def __repr__(self):
        return f"""<Inv object_id={self.object_id}
    CREATION_DATE: {self.creation_date}
    MODIFY_DATE: {self.modify_date}
    FOLDER_ID: {self.folder_id}
    FOLDER_TYPE: {self.folder_type}
    SUB_TYPE: {self.sub_type}
    OBJECT_NAME: {self.object_name}
    UNIQUEKEY: {self.uniquekey}
    GUID: {self.guid}
    FULL_CONTENT_SIZE: {self.full_content_size}
    NUMBER_OF_PAGES: {self.number_of_pages}
    DOC_DOWNLOAD_STATUS: {self.doc_download_status}
    DOC_VALIDATION_STATUS: {self.doc_validation_status}
    DOC_VALIDATION_DATE: {self.doc_validation_date}
    DOC_DOWNLOAD_DATE: {self.doc_download_date}
    DOCUMENT_DATE: {self.document_date}
    SCAN_DATE: {self.scan_date}
    INDEX_DATE: {self.index_date}
    LOAD_DATE: {self.load_date}
    UPDATE_DATE: {self.update_date}
    COMMENT_DOC_FAILURE: {self.comment_doc_failure}>"""