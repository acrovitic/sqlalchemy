# parent reader class for all other reader objects
import os
import sqlalchemy
from sqlalchemy import exc, update
from typing import List, Union
from sqlalchemy import or_, and_
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session, DeclarativeMeta, Query
from sqlalchemy.sql.expression import BinaryExpression, func
import configparser

class PrimaryReader(object):
    def __init__(self, echo_state: bool = False, table_type: str='app_input'):
        self.echo_state: bool = echo_state
        self.config_file: str = os.path.abspath("./config/primary_config.ini")
        self.config: configparser.RawConfigParser = configparser.RawConfigParser()
        self.config.read(self.config_file)
        self.test_mode: int = self.config.getint('TestModeVar','test_mode')
        self.table_type = table_type
        self.active_env: str = self.config.get('ActiveEnvVar', 'active_env')
        self.cloud = 'cloud_name'
        self.DB_CONN = {
            True: URL.create(
                "postgresql",
                username=self.config.get(f"{self.cloud} TestConnVals", "username"),
                password=self.config.get(f"{self.cloud} TestConnVals", "password"),
                host=self.config.get(f"{self.cloud} TestConnVals", "rds"),
                port=self.config.get(f"{self.cloud} TestConnVals", "port"),
                database=self.config.get(f"{self.cloud} TestConnVals", "service_name")
            ),
            False: URL.create(
                "postgresql",
                username=self.config.get(f"{self.cloud} PreProdConnVals", "username"),
                password=self.config.get(f"{self.cloud} PreProdConnVals", "password"),
                host=self.config.get(f"{self.cloud} PreProdConnVals", "rds"),
                port=self.config.get(f"{self.cloud} PreProdConnVals", "port"),
                database=self.config.get(f"{self.cloud} PreProdConnVals", "service_name")
            )
        }[self.test_mode>0]
        engine_lookup_key = self.test_mode>0
        if self.cloud == 'test_cloud_name':
            engine_lookup_key = self.test_mode==0
        self.engine: Engine = {
            True: sqlalchemy.create_engine(
                self.DB_CONN,
                echo=self.echo_state,
                execution_options={
                    "schema_translate_map": dict(self.config.items(f'{self.cloud} SchemaTranslateMap'))
                }
            ),
            False: sqlalchemy.create_engine(self.DB_CONN, echo=self.echo_state)
        }[engine_lookup_key]
        self.session: Session = None


    def connect(self):
        try:
            pw_tmp = self.DB_CONN.password
            self.engine.driver
            session_maker = sqlalchemy.orm.sessionmaker(bind=self.engine)
            self.session: Session = session_maker()

        except exc.SQLAlchemyError as e:
            print(f"{type(e)} | {str(e)}")
            
            
    def disconnect(self):
        self.session.close()
    
                
    def construct_query_obj(self, queryObject: Union[DeclarativeMeta, List]) -> Query:
        # generate query object from session that either targets all table columns or
        # returns select columns based on queryObject type provided declarativemeta is a table object
        # list would be a list of specific table columns
        if type(queryObject) == DeclarativeMeta:
            query: Query = self.session.query(queryObject)
        else:
            query: Query = self.session.query(*queryObject)
        return query
    
    
    def filter_type_switch(self, filter_flag: str):
        # allows for more dynamically defined query construction
        return {"and": and_, "or": or_}[filter_flag]

    
    def order_type_swtich(self, order_flag: str, order_attr):
        return {"max": order_attr.desc(), "min": order_attr.asc()}[order_flag]
    
    
    def filter_by_column(self, queryObject: Union[DeclarativeMeta, List], cond: BinaryExpression, filter_type: str="or", order: str=None, order_attr=None ) -> List:
        # filter by list of binary expressions against table columns
        query: Query = self.construct_query_obj(queryObject)
        filter_obj = self.filter_type_switch(filter_type) # can only be "and" or "or"
        if order is not None:
            order_attr_sort = self.order_type_swtich(order, order_attr)
            result: List[queryObject] = query.order_by(order_attr_sort).filter(
                (filter_obj(*cond))
            ).all()
        else:
            result: List[queryObject] = query.filter(
                (filter_obj(*cond))
            ).all()
        return result
    
    
    def get_distinct_values(self, queryObject: Union[DeclarativeMeta, List], cond: BinaryExpression, filter_type: str="or") -> List:
        # filter by list of binary expressions against table columns
        query: Query = self.construct_query_obj(queryObject)
        filter_obj = self.filter_type_switch(filter_type) # can only be "and" or "or"
        result: List[queryObject] = query.filter(
            (filter_obj(*cond))
        ).distinct()
        return result
    
    
    def insert_row(self, queryObject: Union[DeclarativeMeta, List]):
        self.session.add(queryObject)
        self.session.commit()
        return

    
    def update_row(self, queryObject: Union[DeclarativeMeta, List], cond: BinaryExpression, update_tuple):
        # cond: column filtering conditional statement. should always match exactly one row
        # update_tuple: queryObject attribute to update w value to update with. example: ('column_name', 'value')
        query: Query = self.construct_query_obj(queryObject)
        record: queryObject = query.filter(*cond).one()
        col_to_update, update_value = update_tuple
        setattr(record, col_to_update, update_value)
        self.session.commit()
        return

    
    def random_sample(self, tableDomainObj: DeclarativeMeta, sample_size: int, conditions: List=[]):
        # uses table.column style filtering for random row selection filtering for ALL conditions if conditions provided.
        query: Query = self.construct_query_obj(tableDomainObj)
        if len(conditions) > 0:
            random_sample: List[tableDomainObj] = query.filter(and_(*conditions)).order_by(func.random()).limit(sample_size).all()
        else:
            random_sample: List[tableDomainObj] = query.order_by(func.random()).limit(sample_size).all()
        return random_sample

    
    def commit_session(self):
        self.session.commit()
        return