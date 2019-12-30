import sqlalchemy
import pandas as pd
from typing import Tuple,Optional
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.relation import Relation
from snowshu.utils import MAX_ALLOWED_DATABASES, MAX_ALLOWED_ROWS
from snowshu.core.models.data_types import DataType
from snowshu.logger import Logger
import time
logger=Logger().logger
class BaseSourceAdapter:

    MAX_ALLOWED_DATABASES=MAX_ALLOWED_DATABASES
    MAX_ALLOWED_ROWS=MAX_ALLOWED_ROWS
    DATA_TYPE_MAPPINGS=dict()
    REQUIRED_CREDENTIALS=tuple()
    ALLOWED_CREDENTIALS=tuple()

    def supported_sample_methods(self)->tuple:
        """a static tuple of sample methods from snowshu.adapters.source_adapters.sample_methods"""
        raise NotImplementedError()

    @property
    def credentials(self)->dict:
        return self._credentials

    @credentials.setter
    def credentials(self,value:Credentials)->None:
        for cred in self.REQUIRED_CREDENTIALS:
            if value.__dict__[cred] == None:
                raise KeyError(f"{self.__class__.__name__} requires missing credential {cred}.")
        ALL_CREDENTIALS = self.REQUIRED_CREDENTIALS+self.ALLOWED_CREDENTIALS
        for val in [val for val in value.__dict__.keys() if (val not in ALL_CREDENTIALS and value.__dict__[val] is not None)]:
            raise KeyError(f"{self.__class__.__name__} received extra argument {val} this is not allowed")

        self._credentials=value
       
    def get_connection(self)->sqlalchemy.engine.base.Engine:
        """ uses the instance credentials to create an engine"""
        if not self.credentials:
            raise KeyError('Adapter.get_connection called before setting Adapter.credentials')
    
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
            logger.error('{this.__class__} adapter does not support data type {source_type}.')
            raise e
