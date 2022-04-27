import urllib.parse
from random import randrange

import pytest
import yaml

import snowshu.core.models.materializations as mz
from snowshu.adapters.source_adapters import BaseSourceAdapter
from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.relation import Relation
from snowshu.exceptions import TooManyRecords
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from tests.assets.integration_test_setup import CREDENTIALS, get_connection_profile
from tests.common import query_equalize, rand_string


@pytest.fixture(scope='session')
def sf_adapter():
    with open(CREDENTIALS) as cred_file:
        credentials = yaml.safe_load(cred_file)

    profile_dict = get_connection_profile(credentials)
    adapter = SnowflakeAdapter()
    adapter.credentials = Credentials(**profile_dict)

    return adapter


def test_directionally_wrap_statement(sf_adapter):
    sampling = BernoulliSampleMethod(50, units='probability')
    query = """SELECT * FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"
            WHERE IS_CURRENTLY_TARGETED = TRUE
            AND SALES_REGION IN ('northeast', 'southeast')
            AND PRIMARY_REGIONAL_CREDIT_PROVIDER = 'mastercard'"""
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "EXTERNAL_DATA", "ADDRESS_REGION_ATTRIBUTES"
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.directionally_wrap_statement(query, relation, sampling)

    assert query_equalize(statement) == query_equalize(f"""
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

    assert len(sf_adapter._safe_query(statement)) > 0


def test_upstream_constraint_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "CHILD_TYPE_2_ITEMS"
    LOCAL_KEY, REMOTE_KEY = "ID", "PARENT_2_ID"
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.upstream_constraint_statement(relation, LOCAL_KEY, REMOTE_KEY)

    assert query_equalize(statement) == query_equalize(f" {LOCAL_KEY} in (SELECT {REMOTE_KEY} FROM \
                {sf_adapter.quoted_dot_notation(relation)})")


def test_population_count_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE"
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf_adapter.population_count_statement(relation)

    assert query_equalize(statement) == query_equalize(
        f"SELECT COUNT(*) FROM {sf_adapter.quoted_dot_notation(relation)}")

    assert len(sf_adapter._safe_query(statement)) > 0


def test_get_all_databases(sf_adapter):
    databases_list = ["SNOWSHU_DEVELOPMENT"]

    assert set(databases_list).issubset(set(sf_adapter._get_all_databases()))


def test_get_all_schemas(sf_adapter):
    DATABASE = "SNOWSHU_DEVELOPMENT"
    schemas_set = {"INFORMATION_SCHEMA", "POLYMORPHIC_DATA", "EXTERNAL_DATA", "TESTS_DATA"}

    assert schemas_set.issubset(sf_adapter._get_all_schemas(database=DATABASE))


def test_view_creation_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE"
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
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE"
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

    assert len(sf_adapter._safe_query(statement)) > 0


def test_union_constraint_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE_2"
    subject = Relation(database=DATABASE,
                       schema=SCHEMA,
                       name=TABLE,
                       materialization=TABLE,
                       attributes=[])
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "CHILD_TYPE_1_ITEMS"
    constraint = Relation(database=DATABASE,
                          schema=SCHEMA,
                          name=TABLE,
                          materialization=TABLE,
                          attributes=[])
    max_number_of_outliers = randrange(10)
    subject_key, constraint_key = "ID", "PARENT_2_ID"
    statement = sf_adapter.union_constraint_statement(subject, constraint, subject_key, constraint_key,
                                                      max_number_of_outliers)

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

    assert len(sf_adapter._safe_query(statement)) > 0


def test_polymorphic_constraint_statement(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE"
    LOCAL_KEY, REMOTE_KEY = "ID", "CHILD_ID"
    LOCAL_TYPE, TYPE_MATCH_VAL = "CHILD_TYPE", "type_2"
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


def test_build_conn_string(sf_adapter):
    conn_string = sf_adapter._build_conn_string()
    DATABASE = sf_adapter.credentials.database
    USER = sf_adapter.credentials.user
    PASSWORD = sf_adapter.credentials.password
    ACCOUNT = sf_adapter.credentials.account
    ROLE = sf_adapter.credentials.role

    assert str(conn_string) == f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/{DATABASE}/?role={ROLE}'


def test_count_query(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE"
    query = f'SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE}'
    assert sf_adapter._count_query(query) == 14


def test_get_relations_from_database(sf_adapter):
    SCHEMA_OBJ = BaseSourceAdapter._DatabaseObject("POLYMORPHIC_DATA",
                                                   Relation("snowshu_development", "POLYMORPHIC_DATA", "", None, None))
    relations_list = [Relation("snowshu_development", "POLYMORPHIC_DATA", "PARENT_TABLE_2", mz.TABLE, None),
                      Relation("snowshu_development", "POLYMORPHIC_DATA", "CHILD_TYPE_2_ITEMS", mz.TABLE, None),
                      Relation("snowshu_development", "POLYMORPHIC_DATA", "PARENT_TABLE", mz.TABLE, None),
                      Relation("snowshu_development", "POLYMORPHIC_DATA", "CHILD_TYPE_1_ITEMS", mz.TABLE, None),
                      Relation("snowshu_development", "POLYMORPHIC_DATA", "CHILD_TYPE_0_ITEMS", mz.TABLE, None)]
    received_relations_list = sf_adapter._get_relations_from_database(schema_obj=SCHEMA_OBJ)
    relations_list.sort(key=lambda relation_item: relation_item.name)
    received_relations_list.sort(key=lambda relation_item: relation_item.name)

    assert received_relations_list == relations_list


def test_sample_statement_from_relation(sf_adapter):
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "EXTERNAL_DATA", "ADDRESS_REGION_ATTRIBUTES"
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

    assert len(sf_adapter._safe_query(sample)) > 0


def test_analyze_wrap_statement(sf_adapter):
    DATABASE, SCHEMA, NAME, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE", "PARENT_TABLE"
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
    DATABASE, SCHEMA, TABLE = "SNOWSHU_DEVELOPMENT", "POLYMORPHIC_DATA", "PARENT_TABLE"
    LOCAL_KEY, REMOTE_KEY = "ID", "CHILD_ID"
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
           f'snowflake://{USER}:{PASSWORD.replace("@", urllib.parse.quote_plus("@"))}@{ACCOUNT}/{DATABASE}/?role={ROLE}'


def test_check_count_and_query(sf_adapter):
    query = 'SELECT CHILD_ID from "SNOWSHU_DEVELOPMENT"."POLYMORPHIC_DATA"."PARENT_TABLE"'
    with pytest.raises(TooManyRecords) as exc:
        sf_adapter.check_count_and_query(query, 10, False)

    assert exc.errisinstance(TooManyRecords)
    assert sf_adapter.check_count_and_query.retry.statistics["attempt_number"] == 4
