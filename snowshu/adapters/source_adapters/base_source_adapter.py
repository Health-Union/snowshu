import sqlalchemy
from snowshu.adapters import BaseSQLAdapter
import pandas as pd
from typing import Tuple,Optional
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.relation import Relation
from snowshu.utils import MAX_ALLOWED_DATABASES, MAX_ALLOWED_ROWS
from snowshu.core.models.data_types import DataType
from snowshu.logger import Logger
import time
logger=Logger().logger
class BaseSourceAdapter(BaseSQLAdapter):

    MAX_ALLOWED_DATABASES=MAX_ALLOWED_DATABASES
    MAX_ALLOWED_ROWS=MAX_ALLOWED_ROWS
    DATA_TYPE_MAPPINGS=dict()
    SUPPORTED_SAMPLE_METHODS=tuple()

    
    def get_all_databases(self)->Tuple:
        logger.debug('Collecting databases from snowflake...')
        databases=tuple(self._safe_query(self.GET_ALL_DATABASES_SQL)['database_name'].tolist())
        logger.debug(f'Done. Found {len(databases)} databases.')
        return databases

    def all_releations_from_database(self)->Tuple[Relation]:
        """ this function is expected to get all the non-system relations as a tuple of 
            relation objects for a given database"""
        raise NotImplementedError()       

    def _safe_query(self,query_sql:str)->pd.DataFrame:
        """runs the query and closes the connection"""
        logger.debug('Beginning query execution...')
        start=time.time()
        try:
            conn=self.get_connection()
            cursor=conn.connect()
            # we make the STRONG assumption that all responses will be small enough to live in-memory (because sampling engine).
            # further safety added by the constraints in snowshu.utils
            # this allows the connection to return to the pool
            logger.debug(f'Executed query in {time.time()-start} seconds.')
            frame=pd.read_sql(query_sql,conn)
        finally:
            cursor.close()
            conn.dispose()
        return frame

    def _count_query(self)->int:
        """wraps any query in a COUNT statement, returns that integer"""
        raise NotImplementedError()              

    def check_count_and_query(self,query:str,max_count:int)->tuple:
        """ checks the count, if count passes returns results as a tuple."""
        raise NotImplementedError()

    def _get_data_type(self,source_type:str)->DataType:
        try:
            return self.DATA_TYPE_MAPPINGS[source_type.lower()]
        except KeyError as e:
            logger.error('{self.CLASSNAME} adapter does not support data type {source_type}.')
            raise e
