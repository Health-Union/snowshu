import time
import pandas as pd
import sqlalchemy
from overrides import overrides
from snowshu.exceptions import TooManyRecords
from sqlalchemy.pool import NullPool
from typing import List, Union, Any, Optional, Iterable, Tuple
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.relation import Relation
from snowshu.adapters.source_adapters import BaseSourceAdapter
import snowshu.core.models.data_types as dtypes
import snowshu.core.models.materializations as mz
from snowshu.logger import Logger, duration
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from snowshu.core.models.credentials import USER, PASSWORD, ACCOUNT, DATABASE, SCHEMA, ROLE, WAREHOUSE
logger = Logger().logger


class SnowflakeAdapter(BaseSourceAdapter):
    """The Snowflake Data Warehouse source adapter.
    
    Args:
        preserve_case: By default the adapter folds case-insensitive strings to lowercase.
                       If preserve_case is True,SnowShu will __not__ alter cases (dangerous!).
    """ 
    def __init__(self,preserve_case:bool=False):
        super().__init__(preserve_case)

    name='snowflake'
    SUPPORTS_CROSS_DATABASE=True
    SUPPORTED_FUNCTIONS=set(['ANY_VALUE','RLIKE'])
    SUPPORTED_SAMPLE_METHODS = (BernoulliSampleMethod,)
    REQUIRED_CREDENTIALS = (USER, PASSWORD, ACCOUNT, DATABASE,)
    ALLOWED_CREDENTIALS = (SCHEMA, WAREHOUSE, ROLE,)
    DEFAULT_CASE='lower' ## snowflake in-db is UPPER, but connector is actually lower :(


    DATA_TYPE_MAPPINGS={
        "array":dtypes.JSON,
        "bigint":dtypes.BIGINT,
        "binary":dtypes.BINARY,
        "boolean":dtypes.BOOLEAN,
        "char":dtypes.CHAR,
        "character":dtypes.CHAR,
        "date":dtypes.DATE,
        "datetime":dtypes.DATETIME,
        "decimal":dtypes.DECIMAL,
        "double":dtypes.FLOAT,
        "double precision":dtypes.FLOAT,
        "float":dtypes.FLOAT,
        "float4":dtypes.FLOAT,
        "float8":dtypes.FLOAT,
        "int":dtypes.BIGINT,
        "integer":dtypes.BIGINT,
        "number":dtypes.BIGINT,
        "numeric":dtypes.NUMERIC,
        "object":dtypes.JSON,
        "real":dtypes.FLOAT,
        "smallint":dtypes.BIGINT,
        "string":dtypes.VARCHAR,
        "text":dtypes.VARCHAR,
        "time":dtypes.TIME,
        "timestamp":dtypes.TIMESTAMP_NTZ,
        "timestamp_ntz":dtypes.TIMESTAMP_NTZ,
        "timestamp_ltz":dtypes.TIMESTAMP_TZ,
        "timestamp_tz":dtypes.TIMESTAMP_TZ,
        "varbinary":dtypes.BINARY,
        "varchar":dtypes.VARCHAR,
        "variant":dtypes.JSON}


    MATERIALIZATION_MAPPINGS = {"BASE TABLE": mz.TABLE,
                                "VIEW": mz.VIEW}

    @overrides
    def _get_all_databases(self) -> List[str]:
        """ Use the SHOW api to get all the available db structures."""
        logger.debug('Collecting databases from snowflake...')
        show_result = tuple(self._safe_query("SHOW TERSE DATABASES")['name'].tolist())
        databases = list(set(show_result))
        logger.debug(f'Done. Found {len(databases)} databases.')
        return databases

    @overrides
    def _get_all_schemas(self, database: str) -> List[str]:
        logger.debug(f'Collecting schemas from {database} in snowflake...')
        show_result = self._safe_query(f'SHOW TERSE SCHEMAS IN DATABASE {database}')['name'].tolist()
        schemas = set(show_result)
        logger.debug(f'Done. Found {len(schemas)} schemas in {database} database.')
        return schemas

    def population_count_statement(self,relation:Relation)->str:
        """creates the count * statement for a relation

        Args:
            relation: the :class:`Relation <snowshu.core.models.relation.Relation>` to create the statement for.
        Returns:
            a query that results in a single row, single column, integer value of the unsampled relation population size
        """
        return f"SELECT COUNT(*) FROM {relation.quoted_dot_notation}"

    def view_creation_statement(self, relation: Relation) -> str:
        return f"""
SELECT    
SUBSTRING(GET_DDL('view','{relation.quoted_dot_notation}'),
POSITION(' AS ' IN UPPER(GET_DDL('view','{relation.quoted_dot_notation}')))+3)
"""

    def unsampled_statement(self, relation: Relation) -> str:
        return f"""
SELECT
    *
FROM
    {relation.quoted_dot_notation}
"""

    def directionally_wrap_statement(
            self, sql: str, 
            relation:Relation, 
            sample_type: Union['BaseSampleMethod', None]) -> str:
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

    def analyze_wrap_statement(self, sql: str, relation: Relation) -> str:
        return f"""
WITH
    {relation.scoped_cte('SNOWSHU_COUNT_POPULATION')} AS (
SELECT
    COUNT(*) AS population_size
FROM
    {relation.quoted_dot_notation}
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
    {relation.quoted_dot_notation}
"""
        if sample_type is not None:
            query += f"{self._sample_type_to_query_sql(sample_type)}"
        return query

    def union_constraint_statement( self,
                                    subject:Relation,
                                    constraint:Relation,
                                    subject_key:str,
                                    constraint_key:str,
                                    max_number_of_outliers:int)->str:
        return f"""       
(SELECT
    *
FROM
{subject.quoted_dot_notation}
WHERE 
    {subject_key}
NOT IN 
(SELECT
    {constraint_key}
FROM
{constraint.quoted_dot_notation})
LIMIT {max_number_of_outliers})
"""

    def upstream_constraint_statement(  self,
                                        relation:Relation,
                                        local_key:str,
                                        remote_key:str)->str:
        """ builds upstream where constraints against downstream full population"""
        return f" {local_key} in (SELECT {remote_key} FROM {relation.quoted_dot_notation})" 

    def predicate_constraint_statement( self,
                                        relation:Relation,      
                                        analyze:bool,
                                        local_key:str,
                                        remote_key:str)->str:
        """builds 'where' strings"""
        constraint_sql = str()
        if analyze:
            constraint_sql = f" SELECT {remote_key} AS {local_key} FROM ({relation.core_query})"
        else:

            def quoted(val: Any) -> str:
                return f"'{val}'" if relation.lookup_attribute(
                    remote_key).data_type.requires_quotes else str(val)
            try:
                constraint_set = [
                    quoted(val) for val in relation.data[remote_key].unique()]
                constraint_sql = ','.join(constraint_set)
            except KeyError as e:
                logger.critical(
                    f'failed to build predicates for {relation.dot_notation}: remote key {remote_key} not in dataframe columns ({relation.data.columns})')
                raise e

        return f"{local_key} IN ({constraint_sql}) "

    def _sample_type_to_query_sql(self, sample_type: 'BaseSampleMethod') -> str:
        if sample_type.name == 'BERNOULLI':
            qualifier=sample_type.probability if sample_type.probability\
                        else str(sample_type.rows) + ' ROWS'
            return f"SAMPLE BERNOULLI ({qualifier})"
        elif sample_type.name == 'SYSTEM':
            return f"SAMPLE SYSTEM ({sample_type.probability})"
        else:
            message = f"{sample_type.name} is not supported for SnowflakeAdapter"
            logger.error(message)
            raise NotImplementedError(message)

    @overrides
    def _build_conn_string(self, overrides: Optional[dict] = {}) -> str:
        """overrides the base conn string."""
        conn_parts = [
            f"snowflake://{self.credentials.user}:{self.credentials.password}@{self.credentials.account}/{self.credentials.database}/"]
        conn_parts.append(
            self.credentials.schema if self.credentials.schema is not None else '')
        get_args = list()
        for arg in ('warehouse', 'role',):
            if self.credentials.__dict__[arg] is not None:
                get_args.append(f"{arg}={self.credentials.__dict__[arg]}")

        get_string = "?" + "&".join([arg for arg in get_args])
        return (''.join(conn_parts)) + get_string

    @overrides
    def _get_relations_from_database(self, schema_obj: BaseSourceAdapter._DatabaseObject) -> List[Relation]:
        quoted_database = schema_obj.full_relation.quoted(schema_obj.full_relation.database)  # quoted db name
        relation_database = schema_obj.full_relation.database  # case corrected db name
        case_sensitive_schema = schema_obj.case_sensitive_name  # case sensitive schema name
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
            f'Done collecting relations. Found a total of {len(unique_relations)} unique relations in database {quoted_database}')
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

            relation = Relation(relation_database,
                                self._correct_case(attribute.schema),
                                self._correct_case(attribute.relation),
                                self.MATERIALIZATION_MAPPINGS[attribute.materialization],
                                attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)

        logger.debug(
            f'Acquired {len(relations)} total relations from database {quoted_database}.')
        return relations

    @overrides
    def _count_query(self, query: str) -> int:
        count_sql = f"WITH __SNOWSHU__COUNTABLE__QUERY as ({query}) SELECT COUNT(*) AS count FROM __SNOWSHU__COUNTABLE__QUERY"
        count = int(self._safe_query(count_sql).iloc[0]['count'])
        return count

    @overrides
    def check_count_and_query(self, query: str,
                              max_count: int) -> pd.DataFrame:
        """checks the count, if count passes returns results as a dataframe."""
        try:
            logger.debug('Checking count for query...')
            start_time = time.time()
            count = self._count_query(query)
            assert count <= max_count
            logger.debug(
                f'Query count safe at {count} rows in {time.time()-start_time} seconds.')
        except AssertionError:
            message = f'failed to execute query, result would have returned {count} rows but the max allowed rows for this type of query is {max_count}.'
            logger.error(message)
            logger.debug(f'failed sql: {query}')
            raise TooManyRecords(message)
        response = self._safe_query(query)
        return response

    @overrides
    def get_connection(
            self,
            database_override: Optional[str] = None,
            schema_override: Optional[str] = None) -> sqlalchemy.engine.base.Engine:
        """Creates a connection engine without transactions.

        By default uses the instance credentials unless database or
        schema override are provided.
        """
        if not self._credentials:
            raise KeyError(
                'Adapter.get_connection called before setting Adapter.credentials')

        logger.debug(f'Aquiring {self.CLASSNAME} connection...')
        overrides = dict(
            (k,
             v) for (
                k,
                v) in dict(
                database=database_override,
                schema=schema_override).items() if v is not None)

        engine = sqlalchemy.create_engine(
            self._build_conn_string(overrides), poolclass=NullPool)
        logger.debug(f'engine aquired. Conn string: {repr(engine.url)}')
        return engine
