from unittest import mock
import pytest
from psycopg2 import OperationalError

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
    relation = Relation(database=DATABASE, schema=SCHEMA,
                        name=TABLE, materialization=TABLE, attributes=[])
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
    DATABASE, SCHEMA, TABLE, LOCAL_KEY, REMOTE_KEY = [
        rand_string(10) for _ in range(5)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    
    statement = sf.upstream_constraint_statement(
        relation, LOCAL_KEY, REMOTE_KEY)
    assert query_equalize(statement) == query_equalize(f" {LOCAL_KEY} in (SELECT {REMOTE_KEY} FROM \
                {sf.quoted_dot_notation(relation)})")


def test_population_count_statement():
    sf = SnowflakeAdapter()
    DATABASE, SCHEMA, TABLE = [
        rand_string(10) for _ in range(3)]
    relation = Relation(database=DATABASE,
                        schema=SCHEMA,
                        name=TABLE,
                        materialization=TABLE,
                        attributes=[])
    statement = sf.population_count_statement(relation)
    assert query_equalize(statement) == query_equalize(f"SELECT COUNT(*) FROM {sf.quoted_dot_notation(relation)}")

