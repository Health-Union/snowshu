from random import randrange
from unittest import mock
import pytest
import yaml
from yaml.loader import SafeLoader
#from psycopg2 import OperationalError

from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.materializations import TABLE
from snowshu.core.models.relation import Relation
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from tests.common import query_equalize, rand_string


def test_directionally_wrap_statement_directional():
    sf = SnowflakeAdapter()
    sampling = BernoulliSampleMethod(50,units='probability')
    query = "SELECT * FROM highly_conditional_query"
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE, 
                        schema=SCHEMA,
                        name=TABLE, 
                        materialization=TABLE, 
                        attributes=[])

    assert query_equalize(sf.directionally_wrap_statement(query,relation, sampling)) == query_equalize(f"""
            WITH
                {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS (
            {query}
            )
            ,{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS (
            SELECT
                *
            FROM
                {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')}
            SAMPLE BERNOULLI (50)
            )
            SELECT 
                *
            FROM 
                {relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')}
            """)


def test_upstream_constraint_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE, LOCAL_KEY, REMOTE_KEY = [rand_string(10) for _ in range(5)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf.upstream_constraint_statement(relation, LOCAL_KEY, REMOTE_KEY)
    
    assert query_equalize(statement) == query_equalize(f" {LOCAL_KEY} in (SELECT {REMOTE_KEY} FROM \
                {sf.quoted_dot_notation(relation)})")


def test_population_count_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf.population_count_statement(relation)
    
    assert query_equalize(statement) == query_equalize(f"SELECT COUNT(*) FROM {sf.quoted_dot_notation(relation)}")


def test_get_all_databases():
    sf = SnowflakeAdapter()
    DATABASE = rand_string(15)
    ROLE = "PUBLIC"
    with open('tests/assets/integration/credentials.yml') as f:
        data_credential = yaml.load(f, Loader=SafeLoader)
        creds = Credentials(user=data_credential["USER"], 
                            password=data_credential["PASSWORD"],
                            account=data_credential["ACCOUNT"],
                            database=DATABASE,
                            role=ROLE)
        sf.credentials = creds
        databases_list = ["HU_DATA", "SNOWFLAKE_SAMPLE_DATA", "DEMO_DB", "UTIL_DB"]
    
        assert databases_list.sort() == sf._get_all_databases().sort()


def test_get_all_schemas():
    sf = SnowflakeAdapter()
    DATABASE = "SNOWFLAKE"
    ROLE = "SYSADMIN"
    with open('tests/assets/integration/credentials.yml') as f:
        data_credential = yaml.load(f, Loader=SafeLoader)
        creds = Credentials(user=data_credential["USER"], 
                            password=data_credential["PASSWORD"],
                            account=data_credential["ACCOUNT"],
                            database=DATABASE,
                            role=ROLE)
        sf.credentials = creds
        schemas_set = ("INFORMATION_SCHEMA", "CORE", "DATA_SHARING_USAGE", "ORGANIZATION_USAGE",
                    "READER_ACCOUNT_USAGE", "ACCOUNT_USAGE")
    
        assert list(schemas_set).sort() == list(sf._get_all_schemas(database=DATABASE)).sort()


def test_view_creation_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf.view_creation_statement(relation)

    assert query_equalize(statement) == query_equalize(f"""
                        SELECT
                        SUBSTRING(GET_DDL('view','{sf.quoted_dot_notation(relation)}'),
                        POSITION(' AS ' IN UPPER(GET_DDL('view','{sf.quoted_dot_notation(relation)}')))+3)
                        """)


def test_unsampled_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf.unsampled_statement(relation)

    assert query_equalize(statement) == query_equalize(f"""
            SELECT
                *
            FROM
                {sf.quoted_dot_notation(relation)}
            """)


def test_union_constraint_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    subject = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    constraint = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    max_number_of_outliers = randrange(10)
    subject_key, constraint_key = [rand_string(10) for _ in range(2)]
    statement = sf.union_constraint_statement(subject, constraint, subject_key, constraint_key, max_number_of_outliers)
    
    assert query_equalize(statement) == query_equalize(f"""
            (SELECT
                *
            FROM
            {sf.quoted_dot_notation(subject)}
            WHERE
                {subject_key}
            NOT IN
            (SELECT
                {constraint_key}
            FROM
            {sf.quoted_dot_notation(constraint)})
            LIMIT {max_number_of_outliers})
            """)


def test_quoted():
    sf = SnowflakeAdapter()
    val = rand_string(10)
    assert val == sf.quoted(val)


def test_quoted_for_spaced_string():
    sf = SnowflakeAdapter()
    val = rand_string(5) + ' ' + rand_string(6)
    assert f'"{val}"' == sf.quoted(val)


def test_polymorphic_constraint_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE, LOCAL_KEY, REMOTE_KEY = [rand_string(10) for _ in range(5)]
    LOCAL_TYPE = "CHAR"
    TYPE_MATCH_VAL = "CHAR"
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    relation.core_query = f"""
        SELECT
            *
        FROM 
            {DATABASE}.{SCHEMA}.{TABLE}
            SAMPLE BERNOULLI (10)
        """
    predicate = sf.predicate_constraint_statement(relation, True, LOCAL_KEY, REMOTE_KEY)
    
    assert f" ({predicate} AND LOWER({LOCAL_TYPE}) = LOWER('{TYPE_MATCH_VAL}') ) " == \
        sf.polymorphic_constraint_statement(relation,
                                         True,
                                         LOCAL_KEY,
                                         REMOTE_KEY,
                                         LOCAL_TYPE,
                                         TYPE_MATCH_VAL)


def test_sample_type_to_query_sql():
    sf = SnowflakeAdapter()
    sample_type = BernoulliSampleMethod(10,units="probability")
    qualifier = sample_type.probability

    assert sf._sample_type_to_query_sql(sample_type) == f"SAMPLE BERNOULLI ({qualifier})"
