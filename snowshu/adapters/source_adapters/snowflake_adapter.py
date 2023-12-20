import logging
import time
from typing import TYPE_CHECKING, List, Optional, Union
from urllib.parse import quote

import pandas as pd
import sqlalchemy
import tenacity
from overrides import overrides
from sqlalchemy.pool import NullPool
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

import snowshu.core.models.data_types as dtypes
import snowshu.core.models.materializations as mz
from snowshu.adapters.source_adapters import BaseSourceAdapter
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.credentials import (ACCOUNT, DATABASE, PASSWORD, ROLE,
                                             SCHEMA, USER, WAREHOUSE)
from snowshu.core.models.relation import Relation
from snowshu.exceptions import TooManyRecords
from snowshu.logger import Logger
from snowshu.samplings.sample_methods import BernoulliSampleMethod

if TYPE_CHECKING:
    from snowshu.core.samplings.bases.base_sample_method import BaseSampleMethod

logger = logging.getLogger(__name__)


class SnowflakeAdapter(BaseSourceAdapter):
    """The Snowflake Data Warehouse source adapter.

    Args:
        preserve_case: By default the adapter folds case-insensitive strings to uppercase.
                       If preserve_case is True,SnowShu will __not__ alter cases (dangerous!).
    """

    name = 'snowflake'
    SUPPORTS_CROSS_DATABASE = True
    SUPPORTED_FUNCTIONS = set(['ANY_VALUE', 'RLIKE', 'UUID_STRING'])
    SUPPORTED_SAMPLE_METHODS = (BernoulliSampleMethod,)
    REQUIRED_CREDENTIALS = (USER, PASSWORD, ACCOUNT, DATABASE,)
    ALLOWED_CREDENTIALS = (SCHEMA, WAREHOUSE, ROLE,)
    # snowflake in-db is UPPER, but connector is actually lower :(
    DEFAULT_CASE = 'upper'
    SNOWFLAKE_MAX_NUMBER_EXPR = 16384

    DATA_TYPE_MAPPINGS = {
        "array": dtypes.JSON,
        "bigint": dtypes.BIGINT,
        "binary": dtypes.BINARY,
        "boolean": dtypes.BOOLEAN,
        "char": dtypes.CHAR,
        "character": dtypes.CHAR,
        "date": dtypes.DATE,
        "datetime": dtypes.DATETIME,
        "decimal": dtypes.DECIMAL,
        "double": dtypes.FLOAT,
        "double precision": dtypes.FLOAT,
        "float": dtypes.FLOAT,
        "float4": dtypes.FLOAT,
        "float8": dtypes.FLOAT,
        "int": dtypes.BIGINT,
        "integer": dtypes.BIGINT,
        "number": dtypes.BIGINT,
        "numeric": dtypes.NUMERIC,
        "object": dtypes.JSON,
        "real": dtypes.FLOAT,
        "smallint": dtypes.BIGINT,
        "string": dtypes.VARCHAR,
        "text": dtypes.VARCHAR,
        "time": dtypes.TIME,
        "timestamp": dtypes.TIMESTAMP_NTZ,
        "timestamp_ntz": dtypes.TIMESTAMP_NTZ,
        "timestamp_ltz": dtypes.TIMESTAMP_TZ,
        "timestamp_tz": dtypes.TIMESTAMP_TZ,
        "varbinary": dtypes.BINARY,
        "varchar": dtypes.VARCHAR,
        "variant": dtypes.JSON}

    MATERIALIZATION_MAPPINGS = {"BASE TABLE": mz.TABLE,
                                "VIEW": mz.TABLE}

    @overrides
    def _get_all_databases(self) -> List[str]:
        """ Use the SHOW api to get all the available db structures."""
        logger.debug('Collecting databases from snowflake...')
        show_result = tuple(self._safe_query(
            "SHOW TERSE DATABASES")['name'].tolist())
        databases = list(set(show_result))
        logger.debug(f'Done. Found {len(databases)} databases.')
        return databases

    @overrides
    def _get_all_schemas(self, database: str, exclude_defaults: Optional[bool] = False) -> List[str]:
        database = self.quoted(database)
        logger.debug(f'Collecting schemas from {database} in snowflake...')
        show_result = self._safe_query(f'SHOW TERSE SCHEMAS IN DATABASE {database}')[
            'name'].tolist()
        schemas = set(show_result)
        logger.debug(
            f'Done. Found {len(schemas)} schemas in {database} database.')
        return schemas

    def _get_all_tables(self, database: str, schema: str) -> List[str]:
        database = self.quoted(database)
        schema = self.quoted(schema)
        logger.debug(f'Collecting tables from {schema} schema in {database} database in snowflake...')
        show_result = self._safe_query(f'SHOW TERSE TABLES IN SCHEMA {database}.{schema}')['name'].tolist()
        tables = list(set(show_result))
        logger.debug(f'Done. Found {len(tables)} tables in {schema} schema of {database} database.')
        return tables

    def generate_schema(self, name: str, database: str = 'SNOWSHU'):
        """Create a schema in the specified database.

            Args:
                name: The name of the schema to create.
                database: The database where the schema will be created.
                          Defaults to 'SNOWSHU'.
        """

        corrected_database, corrected_name = (
            self._correct_case(x) for x in (database, name))
        try:
            logger.debug("Creating a schema %s in %s database...",
                         corrected_name, corrected_database)
            query = f'''CREATE TRANSIENT SCHEMA IF NOT EXISTS
                    {corrected_database}.{corrected_name}'''
            result = self._safe_query(query)
            logger.info("Schema creation result: %s", result['status'][0])
        except ValueError as err:
            error_message = (
                f"An error occurred while creating the schema {corrected_name} "
                f"in database {corrected_database}: {err}"
            )
            logger.error(error_message)
            raise

    def drop_schema(self, name: str, database: str = 'SNOWSHU'):
        """Drop a schema and all of its contained objects (tables, views,
        stored procedures)

            Args:
                name: The name of the schema to drop
                database: The database name where the schema is located.
                            Defaults to SNOWSHU.
        """
        corrected_database, corrected_name = (
            self._correct_case(x) for x in (database, name))
        try:
            logger.debug("Creating a schema %s in %s database...",
                         corrected_name, corrected_database)
            query = f'''DROP SCHEMA IF EXISTS
                    {corrected_database}.{corrected_name}
                    CASCADE'''
            result = self._safe_query(query)
            logger.info("Schema drop result: %s", result["status"][0])
        except ValueError as err:
            error_message = (
                f"An error occurred while dropping the schema {corrected_name} "
                f"in database {corrected_database}: {err}"
            )
            logger.error(error_message)
            raise

    def create_table(self, query: str, name: str, schema: str, database: str = 'SNOWSHU'):
        corrected_name, corrected_schema, corrected_database = (
            self._correct_case(x) for x in (name, schema, database)
        )
        full_query = f'''CREATE TRANSIENT TABLE IF NOT EXISTS
            {corrected_database}.{corrected_schema}.{corrected_name}
            AS {query}'''
        try:
            logger.debug("Creating table %s in %s.%s...",
                         corrected_name, corrected_schema, corrected_database)
            result = self._safe_query(full_query)
            logger.info("Table creation result: %s", result['status'][0])
        except ValueError as err:
            error_message = (
                f"An error occurred while creating the table {corrected_name} "
                f"in database {corrected_database}: {err}")
            logger.error(error_message)
            raise

    def drop_table(self, name: str, schema: str, database: str = 'SNOWSHU'):
        corrected_name, corrected_schema, corrected_database = (
            self._correct_case(x) for x in (name, schema, database)
        )
        query = f'''DROP TABLE IF EXISTS
            {corrected_database}.{corrected_schema}.{corrected_name}'''
        try:
            logger.debug("Dropping table %s in %s.%s...",
                         corrected_name, corrected_schema, corrected_database)
            result = self._safe_query(query)
            logger.info("Table drop result: %s", result["status"][0])
        except ValueError as err:
            error_message = (
                f"An error occurred while dropping the table {corrected_name} "
                f"in database {corrected_database}: {err}")
            logger.error(error_message)
            raise

    @staticmethod
    def population_count_statement(relation: Relation) -> str:
        """creates the count * statement for a relation

        Args:
            relation: the :class:`Relation <snowshu.core.models.relation.Relation>` to create the statement for.
        Returns:
            a query that results in a single row, single column, integer value of the unsampled relation population size
        """
        adapter = SnowflakeAdapter()
        return f"SELECT COUNT(*) FROM {adapter.quoted_dot_notation(relation)}"

    @staticmethod
    def view_creation_statement(relation: Relation) -> str:
        adapter = SnowflakeAdapter()
        return f"""
SELECT
SUBSTRING(GET_DDL('view','{adapter.quoted_dot_notation(relation)}'),
POSITION(' AS ' IN UPPER(GET_DDL('view','{adapter.quoted_dot_notation(relation)}')))+3)
"""

    @staticmethod
    def unsampled_statement(relation: Relation) -> str:
        adapter = SnowflakeAdapter()
        return f"""
SELECT
    *
FROM
    {adapter.quoted_dot_notation(relation)}
"""

    def directionally_wrap_statement(self,
                                     sql: str,
                                     relation: Relation,
                                     sample_type: Optional['BaseSampleMethod']) -> str:
        if sample_type is None:
            return sql

        return f"""
WITH
{relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS (
{sql}
)
,{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS (
SELECT
    *
FROM
{relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')}
{self._sample_type_to_query_sql(sample_type)}
)
SELECT
    *
FROM
{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')}
"""

    @staticmethod
    def analyze_wrap_statement(sql: str, relation: Relation) -> str:
        adapter = SnowflakeAdapter()
        return f"""
WITH
    {relation.scoped_cte('SNOWSHU_COUNT_POPULATION')} AS (
SELECT
    COUNT(*) AS population_size
FROM
    {adapter.quoted_dot_notation(relation)}
)
,{relation.scoped_cte('SNOWSHU_CORE_SAMPLE')} AS (
{sql}
)
,{relation.scoped_cte('SNOWSHU_CORE_SAMPLE_COUNT')} AS (
SELECT
    COUNT(*) AS sample_size
FROM
    {relation.scoped_cte('SNOWSHU_CORE_SAMPLE')}
)
SELECT
    s.sample_size AS sample_size
    ,p.population_size AS population_size
FROM
    {relation.scoped_cte('SNOWSHU_CORE_SAMPLE_COUNT')} s
INNER JOIN
    {relation.scoped_cte('SNOWSHU_COUNT_POPULATION')} p
ON
    1=1
LIMIT 1
"""

    def sample_statement_from_relation(
            self, relation: Relation, sample_type: Union['BaseSampleMethod', None]) -> str:
        """builds the base sample statment for a given relation."""
        query = f"""
SELECT
    *
FROM
    {self.quoted_dot_notation(relation)}
"""
        if sample_type is not None:
            query += f"{self._sample_type_to_query_sql(sample_type)}"
        return query

    @staticmethod
    def union_constraint_statement(subject: Relation,
                                   constraint: Relation,
                                   subject_key: str,
                                   constraint_key: str,
                                   max_number_of_outliers: int) -> str:
        """ Union statements to select outliers. This does not pull in NULL values. """
        adapter = SnowflakeAdapter()
        return f"""
(SELECT
    *
FROM
{adapter.quoted_dot_notation(subject)}
WHERE
    {subject_key}
NOT IN
(SELECT
    {constraint_key}
FROM
{adapter.quoted_dot_notation(constraint)})
LIMIT {max_number_of_outliers})
"""

    @staticmethod
    def upstream_constraint_statement(relation: Relation,
                                      local_key: str,
                                      remote_key: str) -> str:
        """ builds upstream where constraints against downstream full population"""
        adapter = SnowflakeAdapter()
        return f" {local_key} in (SELECT {remote_key} FROM \
                {adapter.quoted_dot_notation(relation)})"

    def predicate_constraint_statement(
        self, relation: Relation, analyze: bool, local_key: str, remote_key: str
    ) -> str:
        """builds 'where' strings"""
        try:
            if analyze:
                return f"{local_key} IN ( SELECT {remote_key} AS {local_key} FROM ({relation.core_query}))"

            constraint_query = (
                f"SELECT LISTAGG('''' || {remote_key}::VARCHAR || '''', ',') "
                f"FROM ("
                f"    SELECT DISTINCT {remote_key} "
                f"    FROM {relation.temp_dot_notation} "
                f"    LIMIT {SnowflakeAdapter.SNOWFLAKE_MAX_NUMBER_EXPR}"
                f") AS subquery"
            )
            constraint_sql = self._safe_query(constraint_query).iloc[0, 0]
            if not constraint_sql:
                raise ValueError(
                    f"The constraint set for remote key {remote_key} "
                    f"in {relation.temp_dot_notation} is empty."
                )
            return f"{local_key} IN ({constraint_sql})"
        except KeyError as err:
            logger.critical(
                "Failed to build predicates for %s: remote key %s not in %s table.",
                relation.dot_notation,
                remote_key,
                relation.temp_dot_notation,
            )
            raise KeyError(
                f"Remote key {remote_key} not found in {relation.temp_dot_notation} table."
            ) from err
        except IndexError as err:
            logger.critical(
                "Failed to build predicates for %s: the constraint set "
                "is empty, please validate the relation.",
                relation.dot_notation,
            )
            raise IndexError(f"Failed to build predicates: {str(err)}") from err
        except Exception as err:
            logger.critical(
                "An unexpected error occurred while building predicates for %s: %s",
                relation.dot_notation,
                str(err),
            )
            raise

    # pylint: disable=too-many-arguments
    def polymorphic_constraint_statement(self,
                                         relation: Relation,
                                         analyze: bool,
                                         local_key: str,
                                         remote_key: str,
                                         local_type: str,
                                         local_type_match_val: str = None) -> str:
        predicate = SnowflakeAdapter().predicate_constraint_statement(relation, analyze, local_key, remote_key)
        if local_type_match_val:
            type_match_val = local_type_match_val
        else:
            type_match_val = relation.name[:-1] if relation.name[-1].lower() == 's' else relation.name
        return f" ({predicate} AND LOWER({local_type}) = LOWER('{type_match_val}') ) "

    @staticmethod
    def _sample_type_to_query_sql(sample_type: 'BaseSampleMethod') -> str:
        if sample_type.name == 'BERNOULLI':
            qualifier = sample_type.probability if sample_type.probability\
                else str(sample_type.rows) + ' ROWS'
            return f"SAMPLE BERNOULLI ({qualifier})"
        if sample_type.name == 'SYSTEM':
            return f"SAMPLE SYSTEM ({sample_type.probability})"

        message = f"{sample_type.name} is not supported for SnowflakeAdapter"
        logger.error(message)
        raise NotImplementedError(message)

    @staticmethod
    def quoted(val: str) -> str:
        return f'"{val}"' if ' ' in val else val

    # TODO: change arg name in parent to the fix issue here
    @overrides
    def _build_conn_string(self, overrides: Optional[dict] = None) -> str:  # noqa pylint: disable=redefined-outer-name
        """overrides the base conn string."""
        conn_parts = [f"snowflake://{quote(self.credentials.user)}:{quote(self.credentials.password)}"
                      f"@{quote(self.credentials.account)}/{quote(self.credentials.database)}/",
                      quote(self.credentials.schema) if self.credentials.schema is not None else '']
        get_args = []
        for arg in ('warehouse', 'role',):
            if self.credentials.__dict__[arg] is not None:
                get_args.append(f"{arg}={quote(self.credentials.__dict__[arg])}")

        get_string = "?" + "&".join(get_args)
        return (''.join(conn_parts)) + get_string

    @overrides
    def _get_relations_from_database(
            self, schema_obj: BaseSourceAdapter._DatabaseObject) -> List[Relation]:
        quoted_database = self.quoted(schema_obj.full_relation.database)  # quoted db name
        case_sensitive_schema = schema_obj.case_sensitive_name  # case sensitive schame name
        relations_sql = f"""
                                 SELECT
                                    m.table_schema AS schema,
                                    m.table_name AS relation,
                                    m.table_type AS materialization,
                                    c.column_name AS attribute,
                                    c.ordinal_position AS ordinal,
                                    c.data_type AS data_type
                                 FROM
                                    {quoted_database}.INFORMATION_SCHEMA.TABLES m
                                 INNER JOIN
                                    {quoted_database}.INFORMATION_SCHEMA.COLUMNS c
                                 ON
                                    c.table_schema = m.table_schema
                                 AND
                                    c.table_name = m.table_name
                                 WHERE
                                    m.table_schema = '{case_sensitive_schema}'
                                    AND m.table_schema <> 'INFORMATION_SCHEMA'
                              """

        logger.debug(
            f'Collecting detailed relations from database {quoted_database}...')
        relations_frame = self._safe_query(relations_sql)
        unique_relations = (
            relations_frame['schema'] +
            '.' +
            relations_frame['relation']).unique().tolist()
        logger.debug(
            f'Done collecting relations. Found a total of {len(unique_relations)} '
            f'unique relations in database {quoted_database}')
        relations = list()
        for relation in unique_relations:
            logger.debug(f'Building relation { quoted_database + "." + relation }...')
            attributes = list()

            for attribute in relations_frame.loc[(
                    relations_frame['schema'] + '.' + relations_frame['relation']) == relation].itertuples():
                logger.debug(
                    f'adding attribute {attribute.attribute} to relation..')
                attributes.append(
                    Attribute(
                        self._correct_case(attribute.attribute),
                        self._get_data_type(attribute.data_type)
                    ))

            relation = Relation(schema_obj.full_relation.database,
                                self._correct_case(attribute.schema),   # noqa pylint: disable=undefined-loop-variable
                                self._correct_case(attribute.relation),   # noqa pylint: disable=undefined-loop-variable
                                self.MATERIALIZATION_MAPPINGS[attribute.materialization],   # noqa pylint: disable=undefined-loop-variable
                                attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)

        logger.debug(
            f'Acquired {len(relations)} total relations from database {quoted_database}.')
        return relations

    @overrides
    def _count_query(self, query: str) -> int:
        count_sql = f"WITH __SNOWSHU__COUNTABLE__QUERY as ({query}) \
                    SELECT COUNT(*) AS count FROM __SNOWSHU__COUNTABLE__QUERY"
        count = int(self._safe_query(count_sql).iloc[0]['count'])
        return count

    @tenacity.retry(wait=wait_exponential(),
                    stop=stop_after_attempt(4),
                    before_sleep=Logger().log_retries,
                    reraise=True)
    @overrides
    def check_count_and_query(self, query: str,
                              max_count: int,
                              unsampled: bool) -> pd.DataFrame:
        """checks the count, if count passes returns results as a dataframe."""
        try:
            logger.debug('Checking count for query...')
            start_time = time.time()
            count = self._count_query(query)
            if unsampled and count > max_count:
                warn_msg = (f'Unsampled relation has {count} rows which is over '
                            f'the max allowed rows for this type of query ({max_count}). '
                            f'All records will be loaded into replica.')
                logger.warning(warn_msg)
            else:
                assert count <= max_count
            logger.debug(
                f'Query count safe at {count} rows in {time.time()-start_time} seconds.')
        except AssertionError as exc:
            message = (f'failed to execute query, result would have returned {count} rows '
                       f'but the max allowed rows for this type of query is {max_count}.')
            logger.error(message)
            logger.debug(f'failed sql: {query}')
            raise TooManyRecords(message) from exc
        response = self._safe_query(query)
        return response

    @overrides
    def get_connection(
            self,
            database_override: Optional[str] = None,
            schema_override: Optional[str] = None) -> sqlalchemy.engine.base.Engine:
        """Creates a connection engine without transactions.

        By default, uses the instance credentials unless database or
        schema override are provided.
        """
        if not self._credentials:
            raise KeyError(
                'Adapter.get_connection called before setting Adapter.credentials')

        logger.debug(f'Acquiring {self.CLASSNAME} connection...')
        overrides = {  # noqa pylint: disable=redefined-outer-name
            'database': database_override,
            'schema': schema_override
        }
        overrides = {k: v for k, v in overrides.items() if v is not None}

        engine = sqlalchemy.create_engine(
            self._build_conn_string(overrides), poolclass=NullPool)
        logger.debug(f'Engine acquired. Conn string: {repr(engine.url)}')
        return engine
