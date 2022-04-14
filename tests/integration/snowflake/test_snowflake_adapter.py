from random import randrange

import pytest
import yaml

from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.relation import Relation
from snowshu.exceptions import TooManyRecords
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from snowshu.adapters.source_adapters import BaseSourceAdapter
import snowshu.core.models.materializations as mz
import urllib.parse
from tests.assets.integration_test_setup import CREDENTIALS, get_connection_profile
from tests.common import query_equalize, rand_string


@pytest.fixture(scope='session')
def sf_adapter():
    with open(CREDENTIALS) as cred_file:
        credentials = yaml.safe_load(cred_file)
    
    profile_dict = get_connection_profile(credentials)
    profile_dict.update({"role": "public"})
    adapter = SnowflakeAdapter()
    adapter.credentials = Credentials(**profile_dict)
    return adapter


def test_directionally_wrap_statement_directional(sf_adapter):
    sampling = BernoulliSampleMethod(50,units='probability')
    query = "SELECT * FROM highly_conditional_query"
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE, 
                        schema=SCHEMA,
                        name=TABLE, 
                        materialization=TABLE, 
                        attributes=[])

    assert query_equalize(sf_adapter.directionally_wrap_statement(query,relation, sampling)) == query_equalize(f"""
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


def test_upstream_constraint_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE, LOCAL_KEY, REMOTE_KEY = [rand_string(10) for _ in range(5)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.upstream_constraint_statement(relation, LOCAL_KEY, REMOTE_KEY)
    
    assert query_equalize(statement) == query_equalize(f" {LOCAL_KEY} in (SELECT {REMOTE_KEY} FROM \
                {sf_adapter.quoted_dot_notation(relation)})")


def test_population_count_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.population_count_statement(relation)
    
    assert query_equalize(statement) == query_equalize(f"SELECT COUNT(*) FROM {sf_adapter.quoted_dot_notation(relation)}")


def test_get_all_databases(sf_adapter):
    sf_adapter.credentials.role = 'public'
    databases_list = ["HU_DATA", "SNOWFLAKE_SAMPLE_DATA", "DEMO_DB", "UTIL_DB"]
    
    assert databases_list.sort() == sf_adapter._get_all_databases().sort()


def test_get_all_schemas(sf_adapter):
    DATABASE = "SNOWFLAKE"
    sf_adapter.credentials.role = 'sysadmin'
    schemas_set = {"INFORMATION_SCHEMA", "CORE", "DATA_SHARING_USAGE", "ORGANIZATION_USAGE",
                    "READER_ACCOUNT_USAGE", "ACCOUNT_USAGE"}
    
    assert schemas_set == sf_adapter._get_all_schemas(database=DATABASE)


def test_view_creation_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.view_creation_statement(relation)

    assert query_equalize(statement) == query_equalize(f"""
                        SELECT
                        SUBSTRING(GET_DDL('view','{sf_adapter.quoted_dot_notation(relation)}'),
                        POSITION(' AS ' IN UPPER(GET_DDL('view','{sf_adapter.quoted_dot_notation(relation)}')))+3)
                        """)


def test_unsampled_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = [rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.unsampled_statement(relation)

    assert query_equalize(statement) == query_equalize(f"""
            SELECT
                *
            FROM
                {sf_adapter.quoted_dot_notation(relation)}
            """)


def test_union_constraint_statement(sf_adapter):
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
    statement = sf_adapter.union_constraint_statement(subject, constraint, subject_key, constraint_key, max_number_of_outliers)
    
    assert query_equalize(statement) == query_equalize(f"""
            (SELECT
                *
            FROM
            {sf_adapter.quoted_dot_notation(subject)}
            WHERE
                {subject_key}
            NOT IN
            (SELECT
                {constraint_key}
            FROM
            {sf_adapter.quoted_dot_notation(constraint)})
            LIMIT {max_number_of_outliers})
            """)


def test_quoted(sf_adapter):
    val = rand_string(10)
    assert val == sf_adapter.quoted(val)


def test_quoted_for_spaced_string(sf_adapter):
    val = rand_string(5) + ' ' + rand_string(6)
    assert f'"{val}"' == sf_adapter.quoted(val)


def test_polymorphic_constraint_statement(sf_adapter):
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
    predicate = sf_adapter.predicate_constraint_statement(relation, True, LOCAL_KEY, REMOTE_KEY)
    
    assert f" ({predicate} AND LOWER({LOCAL_TYPE}) = LOWER('{TYPE_MATCH_VAL}') ) " == \
        sf_adapter.polymorphic_constraint_statement(relation,
                                         True,
                                         LOCAL_KEY,
                                         REMOTE_KEY,
                                         LOCAL_TYPE,
                                         TYPE_MATCH_VAL)


def test_sample_type_to_query_sql(sf_adapter):
    sample_type = BernoulliSampleMethod(10,units="probability")
    qualifier = sample_type.probability

    assert sf_adapter._sample_type_to_query_sql(sample_type) == f"SAMPLE BERNOULLI ({qualifier})"


def test_build_conn_string(sf_adapter):
    conn_string = sf_adapter._build_conn_string()
    DATABASE = sf_adapter.credentials.database
    USER = sf_adapter.credentials.user
    PASSWORD = sf_adapter.credentials.password
    ACCOUNT = sf_adapter.credentials.account
    ROLE = sf_adapter.credentials.role

    assert str(conn_string) == f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/{DATABASE}/?role={ROLE}'


def test_count_query(sf_adapter):
    DATABASE, SCHEMA, TABLE = "HU_DATA", "PROD", "site_lookup"
    query = f'SELECT * FROM "{DATABASE}"."{SCHEMA}"."{TABLE}"'
    assert sf_adapter._count_query(query) == 47


def test_get_relations_from_database(sf_adapter):
    SCHEMA_OBJ = BaseSourceAdapter._DatabaseObject("TESTS_DATA", Relation("snowshu_development", "TESTS_DATA", "", None, None))
    relations_list = [Relation("snowshu_development", "TESTS_DATA", "DATA_TYPES", mz.TABLE, None),
                    Relation("snowshu_development", "TESTS_DATA", "CASE_TESTING", mz.TABLE, None), 
                    Relation("snowshu_development", "TESTS_DATA", "ORDER_ITEMS_VIEW", mz.VIEW, None)]
    received_relations_list = sf_adapter._get_relations_from_database(schema_obj=SCHEMA_OBJ)
    relations_list.sort(key=lambda relation_item: relation_item.name)
    received_relations_list.sort(key=lambda relation_item: relation_item.name)
    
    assert received_relations_list == relations_list


def test_sample_statement_from_relation(sf_adapter):
    DATABASE, SCHEMA, TABLE = "HU_DATA", "FEATUREDATA2540ENGAGEMENTS_LAKE", "SITE_LOOKUP"
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    sample = sf_adapter.sample_statement_from_relation(relation, BernoulliSampleMethod(10, units="probability"))
    
    assert query_equalize(sample) == query_equalize(f"""
        SELECT
            *
        FROM 
            {DATABASE}.{SCHEMA}.{TABLE}
            SAMPLE BERNOULLI (10)
        """)


def test_analyze_wrap_statement(sf_adapter):
    DATABASE, SCHEMA, NAME, TABLE = "HU_DATA", "PROD", "SITE_LOOKUP", "SITE_LOOKUP"
    relation = Relation(database=DATABASE, 
                        schema=SCHEMA,
                        name=NAME, 
                        materialization=TABLE, 
                        attributes=[])
    sql = f'SELECT * from {DATABASE}.{SCHEMA}.{TABLE}'
    statement = sf_adapter.analyze_wrap_statement(sql, relation)
    
    assert query_equalize(statement) == query_equalize(f"""
        WITH
            {relation.scoped_cte('SNOWSHU_COUNT_POPULATION')} AS (
        SELECT
            COUNT(*) AS population_size
        FROM
            {sf_adapter.quoted_dot_notation(relation)}
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
        """)


def test_predicate_constraint_statement(sf_adapter):
    LOCAL_KEY, REMOTE_KEY = [rand_string(10) for _ in range(2)]
    DATABASE, SCHEMA, TABLE = "HU_DATA", "PROD", "site_lookup"
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
    statement = sf_adapter.predicate_constraint_statement(relation, True, LOCAL_KEY, REMOTE_KEY)
    
    assert query_equalize(statement) == query_equalize(f"""
        {LOCAL_KEY} IN 
            ( SELECT  
                {REMOTE_KEY}
            AS {LOCAL_KEY}
            FROM (
        SELECT
            *
        FROM 
            {DATABASE}.{SCHEMA}.{TABLE}
            SAMPLE BERNOULLI (10)
        ))
        """)


def test_get_connection(sf_adapter):
    conn_string = sf_adapter.get_connection()
    DATABASE = sf_adapter.credentials.database
    USER = sf_adapter.credentials.user
    PASSWORD = sf_adapter.credentials.password
    ACCOUNT = sf_adapter.credentials.account
    ROLE = sf_adapter.credentials.role

    assert conn_string.url.render_as_string(hide_password=False) == \
        f'snowflake://{USER}:{urllib.parse.quote_plus(PASSWORD)}@{ACCOUNT}/{DATABASE}/?role={ROLE}'


def test_check_count_and_query(sf_adapter):
    query = 'SELECT COMMUNITY_NAME from "HU_DATA"."PROD"."site_lookup"'
    with pytest.raises(TooManyRecords) as exc:
        sf_adapter.check_count_and_query(query, 10, False)
    
    assert exc.errisinstance(TooManyRecords)
    assert sf_adapter.check_count_and_query.retry.statistics["attempt_number"] == 4