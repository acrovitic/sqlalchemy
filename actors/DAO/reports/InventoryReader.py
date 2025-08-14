from typing import List, Union
from sqlalchemy_example.domain.reports.Inventory import Inv
from sqlalchemy_example.actors.PrimaryReader import PrimaryReader

import sqlalchemy
from sqlalchemy import exc
from sqlalchemy import or_, and_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, DeclarativeMeta, Query
from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy import select, func


class InvInventoryReader(PrimaryReader):

    def __init__(self):
        self.supported_doctypes = ["K", "P"]
        self.pdoc_conditions_list = [
            Inv.folder_id.regexp_match('P[0-9]{6,9}'),
            Inv.object_id.regexp_match('.*?(.pdf|.doc)'),
            func.lower(Inv.validation_status)=='validated',
            func.upper(Inv.uniquekey).contains("RESPONSE")==False,
            func.upper(Inv.sub_type).contains('RESPONSE')==False,
            Inv.doc_download_date.isnot(None)
            ]
        self.kfile_conditions_list = [
            Inv.folder_id.startswith("K"),
            Inv.object_id.endswith(".pdf"),
            func.lower(Inv.doc_download_status)=='downloaded',
            func.lower(Inv.doc_validation_status)=='validated',
            or_(func.upper(Inv.uniquekey).startswith("/SUPPLEMENT")==False, Inv.uniquekey.is_(None)),
            or_(func.upper(Inv.uniquekey).startswith("/AMENDMENT")==False, Inv.uniquekey.is_(None)),
            or_(func.upper(Inv.sub_type).contains('CORRESPONDENCE')==False, Inv.uniquekey.is_(None)),
            Inv.doc_download_date.isnot(None)
            ]
        self.nonkfile_conditions_list = [
            Inv.folder_id.startswith("K")==False,
            Inv.object_id.endswith(".pdf"),
            func.lower(Inv.doc_download_status)=='downloaded',
            func.lower(Inv.doc_validation_status)=='validated'
        ]

        super(InvInventoryReader, self).__init__()

    def _doctype_assessment(self, docType):
        if len(docType)==1: # if the docType list has only one doctype
            if docType[0] == "all":
                return 1, self.supported_doctypes
            else:
                return 0, docType[0] # return the single string
        else:
            return 1, docType # return the list

    def _condlist_switch(self, docType) -> List:
        switch = {
            "K": self.kfile_conditions_list.copy(),
            "P": self.pdoc_conditions_list.copy()
        }
        return switch[docType]

    def _format_supplement_ids(self, unformatted_supplement_ids):
        formatted_supp_ids = [tuple(supp_id.split("/", 1)) for supp_id in unformatted_supplement_ids]
        return formatted_supp_ids

    def _identify_submission_types(self, subids_to_query):
        all_submission_types = [subid[0] for subid in subids_to_query]
        submission_types = list(set(all_submission_types))
        return submission_types

    def _generate_doctype_and_statement(self, docType, include_and_statement, additional_conditions=None):
        doctype_condlist = self._condlist_switch(docType)
        if additional_conditions is not None:
            doctype_condlist.extend(additional_conditions)
        to_return = {True: and_(*doctype_condlist).self_group(), False: doctype_condlist}
        return to_return[include_and_statement]

    def _get_condlist_by_doctype(self, docType, include_and_statement: bool, additional_conditions=None) -> List:
        condlist_flag, docType = self._doctype_assessment(docType)
        if condlist_flag==0: # if only one doctype, return that doctypes condition list in an and_()
            doctype_and_statement = self._generate_doctype_and_statement(
                docType,
                include_and_statement=include_and_statement,
                additional_conditions=additional_conditions
            )
            return doctype_and_statement
        else: # if 1<doctype, return or_ in dynamically constructed condition lists based on submission types present
            and_statements = []
            for submission_type in docType:
                doctype_and_statement = self._generate_doctype_and_statement(
                    submission_type,
                    include_and_statement=include_and_statement,
                    additional_conditions=additional_conditions
                )
                and_statements.append(doctype_and_statement)
            return and_statements

    def get_folder_ids(self, cond: List, docType: str, return_query: bool=False) -> List:
        # get list of folder_ids for a given series of conditions to query against for related docs
        target_columns = [Inv.creation_date, Inv.folder_id]
        condlist_flag, docType = self._doctype_assessment(docType)
        if condlist_flag==0:
            tmp_cond_list = self._get_condlist_by_doctype(
                docType,
                include_and_statement=False,
                additional_conditions=cond
            )
            query: List[Inv] = self.filter_by_column(target_columns, tmp_cond_list, filter_type="and")
        else:
            tmp_cond_list = self._get_condlist_by_doctype(
                docType,
                include_and_statement=True,
                additional_conditions=cond
            )
            query: List[Inv] = self.filter_by_column(target_columns, tmp_cond_list, filter_type="or")
        if len(query) > 0:
            if return_query is False:
                folder_ids = list(set([record[1] for record in query]))
                return folder_ids
            if return_query is True:
                folder_ids = list(set([record[1] for record in query]))
                return folder_ids, query

    def query_creationdate_range(self,  date_range: List, docType: list=["K"]) -> List:
        # returns all folder_ids of a given range, then use those ids to return all related docs
        # returns ALL docs for a folderid if that id appears in daterange search.
        # this logic does not yet apply to the other methods fo this class
        startDate, endDate = date_range
        daterange_conditions: List[BinaryExpression] = [
            startDate <= Inv.r_creation_date,
            Inv.r_creation_date <= endDate
        ] # create list with date range conditions
        folder_ids: List[str] = self.get_folder_ids(daterange_conditions, docType) # get folder ids for given date range
        query: List[Inv] = self.query_submission_id(folder_ids, docType)
        return query

    def query_creationdate_exact(self, searchDate: str, docType: list=["K"]) -> List:
        doctype_statement = self._get_condlist_by_doctype(docType, include_and_statement=True)
        date_statement = [searchDate == Inv.creation_date]
        condition_list = [doctype_statement, date_statement]
        query: List[Inv] = self.filter_by_column(Inv, condition_list, filter_type="and")
        return query

    def query_r_object_id(self, object_id: List, docType: list=["K"]) -> List:
        # query for singular document of a specific object_id
        docType_statement = self._get_condlist_by_doctype(docType, include_and_statement=True)
        objectid_statement = or_(*[Inv.object_id==oid for oid in object_id])
        condition_list = [docType_statement, objectid_statement]
        query: List[Inv] = self.filter_by_column(Inv, condition_list, filter_type="and")
        return query

    def query_submission_id(self, submission_id: List, docType: list=["K"], order=None, order_attr=None) -> List:
        docType_statement = self._get_condlist_by_doctype(docType, include_and_statement=True)
        submission_types = self._identify_submission_types(submission_id)
        folderid_statement = or_(*[Inv.folder_id==subid for subid in submission_id if "/" not in subid])
        if len(submission_types)>1:
            condition_list = [or_(*docType_statement), folderid_statement]
        else:
            condition_list = [docType_statement, folderid_statement]

        if any("/" in subid for subid in submission_id):
            supplement_ids = [subid for subid in submission_id if "/" in subid]
            other_ids = [subid for subid in submission_id if "/" not in subid]
            formatted_supplement_ids = self._format_supplement_ids(supplement_ids) # returns list of tuples
            supplement_conditions = [
                and_(Inv.folder_id==subid, Inv.uniquekey==f"/{supp}") for subid, supp in formatted_supplement_ids
            ]

            if len(other_ids)>0:
                submission_query: List[Inv] = self.filter_by_column(
                    Inv,
                    condition_list,
                    filter_type="and",
                    order=order,
                    order_attr=order_attr
                )
            else:
                submission_query = list()
            supplement_query: List[Inv] = self.filter_by_column(
                Inv,
                supplement_conditions,
                filter_type="or",
                order=order,
                order_attr=order_attr
            )
            supplement_query_final = [o for o in supplement_query if o.object_id.endswith(".pdf")]
            query = submission_query + supplement_query_final
            return query
        else:
            query: List[Inv] = self.filter_by_column(
                Inv,
                condition_list,
                filter_type="and",
                order=order,
                order_attr=order_attr
            )
            return query

    def query_unique_column_values(self, columnObjectList: Union[DeclarativeMeta, List], docType: list=["K"]) -> List:
        # get count of unique values in a given column
        docType_statement = self._get_condlist_by_doctype(docType)
        condition_list = [docType_statement]
        query: List[Inv] = self.get_distinct_values(columnObjectList, condition_list, filter_type="and")
        return query

    def random_kdoc_sample(self, sample_size: int) -> List:
        query: List[Inv] = self.random_sample(Inv, sample_size, self.kfile_conditions_list)
        return query

    def random_pma_sample(self, sample_size: int) -> List:
        query: List[Inv] = self.random_sample(Inv, sample_size, self.pdoc_conditions_list)
        return query

    def random_nonkdoc_sample(self, sample_size: int) -> List:
        query: List[Inv] = self.random_sample(Inv, sample_size, self.nonkfile_conditions_list)
        return query
    
    def get_max_r_creation_date_by_subid(self, subids: list) -> List:
        condition_lists = self._get_condlist_by_doctype(docType=["all"], include_and_statement=True)
        result = self.session.execute(select(Inv.folder_id, func.max(Inv.creation_date))
                                      .where(Inv.folder_id.in_(subids))
                                      .where(or_(*condition_lists))
                                      .group_by(Inv.folder_id)).all()
        self.session.commit()
        return result

    def get_change_sups(self, subids: list) -> List:
        condition_list = self._get_condlist_by_doctype(docType=['P'],include_and_statement=True)
        query = self.session.execute(select(func.distinct(Inv.folder_id + Inv.uniquekey))
                                    .where(condition_list,
                                            Inv.folder_id.in_(subids),
                                            func.lower(Inv.uniquekey).contains('supplement'),
                                            func.length(Inv.uniquekey) > 1,
                                            Inv.uniquekey.is_not(None),
                                            Inv.creation_date >= '2025-01-01'
                                            )).all()
        self.session.commit()
        pma_change_sups = [record[0] for record in query]
        return pma_change_sups