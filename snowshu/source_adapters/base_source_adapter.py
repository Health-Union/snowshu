import sqlalchemy
from typing import Tuple
from snowshu.core.relation import Relation
from snowshu.utils import MAX_ALLOWED_DATABASES, MAX_ALLOWED_ROWS
from snowshu.core.data_types import DataType
from snowshu.logger import Logger
import time
logger=Logger().logger
class BaseSourceAdapter:

    MAX_ALLOWED_DATABASES=MAX_ALLOWED_DATABASES
    MAX_ALLOWED_ROWS=MAX_ALLOWED_ROWS
    DATA_TYPE_MAPPINGS=dict()

    def supported_sample_methods(self)->tuple:
        """a static tuple of sample methods from snowshu.source_adapters.sample_methods"""
        raise NotImplementedError()

    def get_connection(self, credentials:dict)->sqlalchemy.engine.base.Engine:
        """accepts a dict of credentials and returns a sqlalchemy Engine."""
        raise NotImplementedError()
    
    def get_all_databases(self,credentials:sqlalchemy.engine.base.Engine)->Tuple:
        """gets all non-system databases for a source."""
        raise NotImplementedError()

    def all_releations_from_database(self)->Tuple[Relation]:
        """ this function is expected to get all the non-system relations as a tuple of 
            relation objects for a given database"""
        raise NotImplementedError()       

    def _safe_query(self,conn:sqlalchemy.engine.base.Engine,query_sql:str)->list:
        """runs the query and closes the connection"""
        logger.debug('Beginning query execution...')
        start=time.time()
        try:
            cursor=conn.connect()
            # we make the STRONG assumption that all responses will be small enough to live in-memory (because sampling engine).
            # further safety added by the constraints in snowshu.utils
            # this allows the connection to return to the pool
            logger.debug(f'Executed query in {time.time()-start} seconds.')
            return cursor.execute(query_sql).fetchall()
        finally:
            cursor.close()

    def _count_query(self,connection:sqlalchemy.engine.base.Engine,query:str)->int:
        """wraps any query in a COUNT statement, returns that integer"""
        raise NotImplementedError()              

    def _check_count_and_query(self,connection:sqlalchemy.engine.base.Engine,query:str,max_count:int)->tuple:
        """ checks the count, if count passes returns results as a tuple."""
        raise NotImplementedError()

    def _get_data_type(self,source_type:str)->DataType:
        try:
            return self.DATA_TYPE_MAPPINGS[source_type.lower()]
        except KeyError as e:
            logger.error('{this.__name__} adapter does not support data type {source_type}.')
            raise e
