import copy
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Optional, Set

import pandas as pd
import sqlalchemy
from sqlalchemy.pool import NullPool

from snowshu.core.models import Relation
from snowshu.core.models.credentials import (DATABASE, HOST, PASSWORD, USER,
                                             Credentials)
from snowshu.core.models.relation import at_least_one_full_pattern_match
from snowshu.core.utils import correct_case
from snowshu.logger import Logger, duration

logger = Logger().logger


class BaseSQLAdapter:
    DEFAULT_CASE = 'lower'

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

    def __init__(self, preserve_case: bool = False):
        self.CLASSNAME = self.__class__.__name__     # noqa pylint: disable=invalid-name
        self.preserve_case = preserve_case
        for attr in ('REQUIRED_CREDENTIALS', 'ALLOWED_CREDENTIALS',
                     'MATERIALIZATION_MAPPINGS',):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'SQL adapter requires attribute {attr} but was not set.')

    @property
    def credentials(self) -> dict:
        return self._credentials

    @credentials.setter
    def credentials(self, value: Credentials) -> None:
        for cred in self.REQUIRED_CREDENTIALS:
            if value.__dict__[cred] is None:
                raise KeyError(
                    f"{self.CLASSNAME} requires missing credential {cred}.")
        ALL_CREDENTIALS = self.REQUIRED_CREDENTIALS + self.ALLOWED_CREDENTIALS  # noqa pylint: disable=invalid-name
        for val in [val for val in value.__dict__.keys() if (
                val not in ALL_CREDENTIALS and value.__dict__[val] is not None)]:
            raise KeyError(
                f"{self.CLASSNAME} received extra argument {val} this is not allowed")

        self._credentials = value

    def get_connection(
            self,
            database_override: Optional[str] = None,
            schema_override: Optional[str] = None) -> sqlalchemy.engine.base.Engine:
        """Creates a connection engine without transactions.

        By default uses the instance credentials unless database or
        schema override are provided.
        """
        if not self._credentials:
            raise KeyError('Adapter.get_connection called before setting Adapter.credentials')

        logger.debug(f'Acquiring {self.CLASSNAME} connection...')
        overrides = dict(
            (k, v) for (k, v) in dict(
                database=database_override,
                schema=schema_override).items()
            if v is not None)

        engine = sqlalchemy.create_engine(self._build_conn_string(
            overrides), poolclass=NullPool, isolation_level="AUTOCOMMIT")
        logger.debug(f'engine acquired. Conn string: {repr(engine.url)}')
        return engine

    def _safe_query(self, query_sql: str, database: str = None) -> pd.DataFrame:
        """runs the query and closes the connection."""
        logger.debug('Beginning query execution...')
        start = time.time()
        conn = None
        cursor = None
        try:
            # database_override is needed for databases like postgre
            conn = self.get_connection() if not database else self.get_connection(database_override=database)
            cursor = conn.connect()
            # we make the STRONG assumption that all responses will be small enough
            # to live in-memory (because sampling engine).
            # further safety added by the constraints in snowshu.configs
            # this allows the connection to return to the pool
            logger.debug(f'Executed query in {time.time()-start} seconds.')
            frame = pd.read_sql_query(query_sql, conn)
            logger.debug("Dataframe datatypes: %s", str(frame.dtypes).replace('\n', ' | '))
            if len(frame) > 0:
                for col in frame.columns:
                    logger.debug("Pandas loaded element 0 of column %s as %s", col, type(frame[col][0]))
            else:
                logger.debug("Dataframe is empty")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.dispose()
        return frame

    def _build_conn_string(self, overrides: dict = None) -> str:
        """This is the most basic implementation of a connection string
        possible and is intended to be extended.

        generates a database url per https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
        passes any overrides via override object
        """
        if not hasattr(self, 'dialect'):
            # attempt to infer the dialect
            raise KeyError(
                'base_sql_adapter unable to build connection string; required param `dialect` to infer.')
        if not overrides:
            overrides = {}

        self._credentials.urlencode()
        conn_string, used_credentials = self._build_conn_string_partial(
            self.dialect, overrides.get('database'))
        instance_creds = copy.deepcopy(self._credentials)
        for key in overrides.keys():
            instance_creds.__dict__[key] = overrides[key]
        get_args = '&'.join([f"{arg}={instance_creds.__dict__[arg]}"
                            for arg in (set(self.ALLOWED_CREDENTIALS) - used_credentials)
                            if arg in vars(instance_creds) and instance_creds.__dict__[arg] is not None])
        return conn_string + get_args

    def _build_conn_string_partial(
            self, dialect: str, database: Optional[str] = None) -> tuple:
        """abstracted to make this easier to override.

        RETURNS: a tuple with the conn string and a tuple of credential args used in that string
        """
        database = database if database is not None else self._credentials.database
        return (
            f"{dialect}://{self._credentials.user}:{self._credentials.password}@{self._credentials.host}/{database}?",
            {USER, PASSWORD, HOST, DATABASE, }
        )

    def build_catalog(self, patterns: Iterable[dict], thread_workers: int) -> Set[Relation]:
        """ This function is expected to return all of the relations that satisfy the filters 

            Args:
                patterns (Iterable[dict]): Filter dictionaries to apply to the databases
                    requires "database", "schema", and "name" keys
                thread_workers (int): The number of workers to use when building the catalog

            Returns:
                Set[Relation]: All of the relations from the sql adapter pass the filters
        """
        filtered_schemas = self._get_filtered_schemas(patterns)

        def accumulate_relations(schema_obj: BaseSQLAdapter._DatabaseObject, accumulator):
            try:
                relations = self._get_relations_from_database(schema_obj)
                accumulator += [
                    r for r in relations if at_least_one_full_pattern_match(r, patterns)]
            except Exception as exc:
                logger.critical(exc)
                raise exc

        # get all columns for filtered db/schema
        catalog = []
        logger.info('Building filtered catalog...')
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=thread_workers) as executor:
            for f_schema in filtered_schemas:
                executor.submit(accumulate_relations, f_schema, catalog)

        logger.info(f'Done building catalog. Found a total of {len(catalog)} relations '
                    f'from the database in {duration(start_time)}.')
        return set(catalog)

    def _get_all_databases(self) -> List[str]:
        raise NotImplementedError()

    def _get_all_schemas(self, database: str, exclude_defaults: Optional[bool] = False) -> List[str]:
        """ Returns the raw names of the schemas in the given database (raw case) """
        raise NotImplementedError()

    def _get_filtered_schemas(self, filters: Iterable[dict]) -> List[_DatabaseObject]:
        """ Get all of the filtered schema structures based on the provided filters. """
        db_filters = []
        schema_filters = []
        for _filter in filters:
            new_filter = _filter.copy()
            new_filter["name"] = ".*"
            if schema_filters.count(new_filter) == 0:
                schema_filters.append(new_filter)
        for s_filter in schema_filters:
            new_filter = s_filter.copy()
            new_filter["schema"] = ".*"
            if db_filters.count(new_filter) == 0:
                db_filters.append(new_filter)

        databases = self._get_all_databases()
        database_relations = [Relation(self._correct_case(database), "", "", None, None)
                              for database in databases]
        filtered_databases = [rel for rel in database_relations
                              if at_least_one_full_pattern_match(rel, db_filters)]

        # get all schemas in all databases
        filtered_schemas = []
        for db_rel in filtered_databases:
            schemas = self._get_all_schemas(
                database=db_rel.quoted(db_rel.database))
            schema_objs = [
                BaseSQLAdapter._DatabaseObject(
                    schema, 
                    Relation(db_rel.database, self._correct_case(schema), "", None, None))
                for schema in schemas]
            filtered_schemas += [
                d for d in schema_objs if at_least_one_full_pattern_match(d.full_relation, schema_filters)]

        return filtered_schemas

    def _get_relations_from_database(self, schema_obj: _DatabaseObject):
        raise NotImplementedError()

    def _correct_case(self, val: str) -> str:
        """The base case correction method for a sql adapter.
        """
        return val if self.preserve_case else correct_case(val, self.DEFAULT_CASE == 'upper')
