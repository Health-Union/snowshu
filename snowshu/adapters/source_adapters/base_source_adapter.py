from concurrent.futures import ThreadPoolExecutor
from snowshu.adapters import BaseSQLAdapter
from snowshu.core.utils import correct_case
import pandas as pd
from typing import Tuple, Any, Iterable, List
from snowshu.core.models import Relation, DataType
from snowshu.core.models.relation import at_least_one_full_pattern_match
from snowshu.configs import MAX_ALLOWED_DATABASES, MAX_ALLOWED_ROWS
from snowshu.logger import Logger, duration
import time
logger = Logger().logger


class BaseSourceAdapter(BaseSQLAdapter):
    name =''
    MAX_ALLOWED_DATABASES = MAX_ALLOWED_DATABASES
    MAX_ALLOWED_ROWS = MAX_ALLOWED_ROWS
    DEFAULT_CASE='lower'
    SUPPORTS_CROSS_DATABASE=False
    SUPPORTED_FUNCTIONS=set()

    class _DatabaseObject:
        """ An internal class to allow for preserving name casing when needed

            Ex: When querying information_schema, an object name may need to
                passed as a varchar for a case-sensitive match

            Args:
                case_sensitive_name (str): The name of the object in the original case
                full_relation (Relation): An object that represents the database object
                    to allow for convenient pattern matching
        """
        def __init__(self, case_sensitive_name: str, full_relation: Relation):
            self.case_sensitive_name = case_sensitive_name
            self.full_relation = full_relation

    def __init__(self, preserve_case:bool=False):
        self.preserve_case=preserve_case
        super().__init__()
        for attr in ('DATA_TYPE_MAPPINGS', 'SUPPORTED_SAMPLE_METHODS',):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'Source adapter requires attribute f{attr} but was not set.')

    def build_catalog(self, patterns: Iterable[dict], thread_workers: int = 1) -> Tuple[Relation]:
        """ This function is expected to return all of the relations that satisfy the filters 

            Args:
                patterns (Iterable[dict]): Filter dictionaries to apply to the source databases
                    requires "database", "schema", and "name" keys
                thread_workers (int): The number of workers to use when building the catalog

            Returns:
                Tuple[Relation]: All of the relations from the source adapter pass the filters
        """
        filtered_schemas = self._get_filtered_schemas(patterns)

        def accumulate_relations(schema_obj: BaseSourceAdapter._DatabaseObject, accumulator):
            try:
                relations = self._get_relations_from_database(schema_obj)
                accumulator += [r for r in relations if at_least_one_full_pattern_match(r, patterns)]
            except Exception as e:
                logger.critical(e)
                raise e

        # get all columns for filtered db/schema
        catalog = []
        logger.info('Building filtered catalog...')
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=thread_workers) as executor:
            {executor.submit(accumulate_relations, d, catalog)
            for d in filtered_schemas}

        logger.info(
            f'Done building catalog. Found a total of {len(catalog)} relations from the source in {duration(start_time)}.')
        return tuple(catalog)

    def _get_all_databases(self) -> List[str]:
        raise NotImplementedError()

    def _get_all_schemas(self, database: str) -> List[str]:
        """ Returns the raw names of the schemas in the given database (raw case) """
        raise NotImplementedError()

    def _get_filtered_schemas(self, filters: Iterable[dict]) -> List[_DatabaseObject]:
        """ Get all of the filtered schema structures based on the provided filters. """
        db_filters = []
        schema_filters = []
        for f in filters:
            new_filter = f.copy()
            new_filter["name"] = ".*"
            if schema_filters.count(new_filter) == 0:
                schema_filters.append(new_filter)
        for f in schema_filters:
            new_filter = f.copy()
            new_filter["schema"] = ".*"
            if db_filters.count(new_filter) == 0:
                db_filters.append(new_filter)

        databases = self._get_all_databases()
        database_relations = [Relation(self._correct_case(database), "", "", None, None) for database in databases]
        filtered_databases = [rel for rel in database_relations if at_least_one_full_pattern_match(rel, db_filters)]

        # get all schemas in all databases
        filtered_schemas = []
        for db_rel in filtered_databases:
            schemas = self._get_all_schemas(database=db_rel.quoted(db_rel.database))
            schema_objs = [ BaseSourceAdapter._DatabaseObject(schema,
                                            Relation(db_rel.database, self._correct_case(schema), "", None, None))
                             for schema in schemas ]
            filtered_schemas += [d for d in schema_objs if at_least_one_full_pattern_match(d.full_relation, schema_filters)]

        return filtered_schemas

    def _get_relations_from_database(self, schema_obj: _DatabaseObject):
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

    def _correct_case(self,val:str)->str:
        """The base case correction method for a source adapter.
        """
        return val if self.preserve_case else correct_case(val,self.DEFAULT_CASE=='upper') 

    def _count_query(self, query: str) -> int:
        """wraps any query in a COUNT statement, returns that integer."""
        raise NotImplementedError()

    def check_count_and_query(self, query: str, max_count: int) -> pd.DataFrame:
        """checks the count, if count passes returns results as a dataframe."""
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
