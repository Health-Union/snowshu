from snowshu.adapters import BaseSQLAdapter
import pandas as pd
from typing import Tuple,Any
from snowshu.core.models import Relation, DataType
from snowshu.configs import MAX_ALLOWED_DATABASES, MAX_ALLOWED_ROWS
from snowshu.logger import Logger
import time
logger = Logger().logger


class BaseSourceAdapter(BaseSQLAdapter):
    name =''
    MAX_ALLOWED_DATABASES = MAX_ALLOWED_DATABASES
    MAX_ALLOWED_ROWS = MAX_ALLOWED_ROWS
    SUPPORTS_CROSS_DATABASE=False
    SUPPORTED_FUNCTIONS=set()

    def __init__(self):
        super().__init__()
        for attr in ('DATA_TYPE_MAPPINGS', 'SUPPORTED_SAMPLE_METHODS',):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'Source adapter requires attribute f{attr} but was not set.')

    def get_all_databases(self) -> Tuple:
        logger.debug('Collecting databases from snowflake...')
        databases = tuple(self._safe_query(self.get_all_databases_statement())[
                          'database_name'].tolist())
        logger.debug(f'Done. Found {len(databases)} databases.')
        return databases

    def all_releations_from_database(self) -> Tuple[Relation]:
        """this function is expected to get all the non-system relations as a
        tuple of relation objects for a given database."""
        raise NotImplementedError()

    def _safe_query(self, query_sql: str) -> pd.DataFrame:
        """runs the query and closes the connection."""
        logger.debug('Beginning query execution...')
        start = time.time()
        try:
            conn = self.get_connection()
            cursor = conn.connect()
            # we make the STRONG assumption that all responses will be small enough to live in-memory (because sampling engine).
            # further safety added by the constraints in snowshu.configs
            # this allows the connection to return to the pool
            logger.debug(f'Executed query in {time.time()-start} seconds.')
            frame = pd.read_sql_query(query_sql, conn)
        finally:
            cursor.close()
            conn.dispose()
        return frame

    def _count_query(self) -> int:
        """wraps any query in a COUNT statement, returns that integer."""
        raise NotImplementedError()

    def check_count_and_query(self, query: str, max_count: int) -> tuple:
        """checks the count, if count passes returns results as a tuple."""
        raise NotImplementedError()

    def scalar_query(self, query: str) -> Any:
        """Returns only a single value.
        
        When the database is expected to return a single row with a single column, 
        this method will return the raw value from that cell. Will throw a :class:`TooManyRecords <snowshu.exceptions.TooManyRecords>` exception. 
        
        Args:
            query: the query to execute.

        Returns:
            the raw value from cell [0][0]
        """
        return self.check_count_and_query(query,1).iloc[0][0]           

    def _get_data_type(self, source_type: str) -> DataType:
        try:
            return self.DATA_TYPE_MAPPINGS[source_type.lower()]
        except KeyError as e:
            logger.error(
                '{self.CLASSNAME} adapter does not support data type {source_type}.')
            raise e
