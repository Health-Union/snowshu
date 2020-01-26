from dataclasses import dataclass
import copy
import pytest
import mock
import pandas as pd
from dfmock import DFMock
from tests.common import rand_string
import networkx as nx
from tests.common import query_equalize
import snowshu.core.models.materializations as mz
from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.models.relation import Relation
from snowshu.core.compile import RuntimeSourceCompiler
from snowshu.core.sampling.sample_methods import BernoulliSample
from snowshu.samplings import DefaultSampling
from snowshu.core.models.attribute import Attribute
import snowshu.core.models.data_types as dt


def test_analyze_unsampled(stub_relation_set):
    upstream = stub_relation_set.upstream_relation
    upstream.unsampled = True
    dag = nx.DiGraph()
    dag.add_edges_from([(upstream, stub_relation_set.downstream_relation,)])
    compiler = RuntimeSourceCompiler()
    adapter, unsampled_method = [mock.MagicMock() for _ in range(2)]
    adapter.unsampled_statement = unsampled_method

    result = compiler.compile_queries_for_relation(
        upstream, dag, adapter, True)
    assert unsampled_method.called


def test_analyze_iso(stub_relation_set):
    iso = stub_relation_set.iso_relation
    iso.sample_method = BernoulliSample(10)
    iso.sampling=DefaultSampling()
    iso.sampling.sample_method._rows == 1500
    iso.sampling.sample_method._units == 'rows'
    dag = nx.DiGraph()
    dag.add_nodes_from([iso])
    compiler = RuntimeSourceCompiler()
    adapter = SnowflakeAdapter()
    result = compiler.compile_queries_for_relation(iso, dag, adapter, True)
    assert query_equalize(iso.compiled_query) == query_equalize(f"""
WITH
    {iso.scoped_cte('SNOWSHU_COUNT_POPULATION')} AS (
SELECT
    COUNT(*) AS population_size
FROM
    {iso.quoted_dot_notation}
)
,{iso.scoped_cte('SNOWSHU_CORE_SAMPLE')} AS (
SELECT
    *
FROM 
    {iso.quoted_dot_notation}
    SAMPLE BERNOULLI (1500 ROWS)
)
,{iso.scoped_cte('SNOWSHU_CORE_SAMPLE')}_COUNT AS (
SELECT
    COUNT(*) AS sample_size
FROM
    {iso.scoped_cte('SNOWSHU_CORE_SAMPLE')}
)
SELECT
    s.sample_size AS sample_size
    ,p.population_size AS population_size
FROM
    {iso.scoped_cte('SNOWSHU_CORE_SAMPLE')}_COUNT s
INNER JOIN
    {iso.scoped_cte('SNOWSHU_COUNT_POPULATION')} p
ON
    1=1
LIMIT 1
""")


def test_run_iso(stub_relation_set):
    iso = stub_relation_set.iso_relation
    iso.sample_method = BernoulliSample(10)
    iso.sampling=DefaultSampling()
    iso.sampling.sample_method._rows == 1500
    iso.sampling.sample_method._units == 'rows'
    dag = nx.DiGraph()
    dag.add_nodes_from([iso])
    compiler = RuntimeSourceCompiler()
    adapter = SnowflakeAdapter()
    result = compiler.compile_queries_for_relation(iso, dag, adapter, False)
    assert query_equalize(iso.compiled_query) == query_equalize(f"""
SELECT
    *
FROM 
    {iso.quoted_dot_notation}
    SAMPLE BERNOULLI (1500 ROWS)
""")


def test_run_deps_directional(stub_relation_set):
    upstream=stub_relation_set.upstream_relation
    downstream=stub_relation_set.downstream_relation
    upstream.data=pd.DataFrame([dict(id=1),dict(id=2),dict(id=3)])
    for relation in (downstream,upstream,):
        relation.attributes=[Attribute('id',dt.INTEGER)]
        relation.sample_method=BernoulliSample(10)
        relation.sampling=DefaultSampling()
        relation.sampling.sample_method._rows == 1500
        relation.sampling.sample_method._units == 'rows'
    
    dag=nx.DiGraph()
    dag.add_edge(upstream,downstream,direction="directional",remote_attribute='id',local_attribute='id')
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(upstream,dag,adapter,False)
    compiler.compile_queries_for_relation(downstream,dag,adapter,False)
    assert query_equalize(downstream.compiled_query)==query_equalize(f"""

WITH 
{downstream.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS ( 
SELECT 
    * 
FROM 
{downstream.quoted_dot_notation}
WHERE id IN (1,2,3) 
)
,{downstream.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS ( 
SELECT 
    * 
FROM 
{downstream.scoped_cte('SNOWSHU_FINAL_SAMPLE')} SAMPLE BERNOULLI (1500 ROWS) 
) 
SELECT 
    * 
FROM 
{downstream.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')}
""")


def test_run_deps_bidirectional_include_outliers(stub_relation_set):
    upstream=stub_relation_set.upstream_relation
    downstream=stub_relation_set.downstream_relation
    upstream.data=pd.DataFrame([dict(id=1),dict(id=2),dict(id=3)])
    for relation in (downstream,upstream,):
        relation.attributes=[Attribute('id',dt.INTEGER)]
        relation.sample_method=BernoulliSample(10)
        relation.include_outliers=True    
        relation.max_number_of_outliers=100
        relation.sampling=DefaultSampling()
        relation.sampling.sample_method._rows == 1500
        relation.sampling.sample_method._units == 'rows'

    dag=nx.DiGraph()
    dag.add_edge(upstream,downstream,direction="bidirectional",remote_attribute='id',local_attribute='id')
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(upstream,dag,adapter,False)
    compiler.compile_queries_for_relation(downstream,dag,adapter,False)
    assert query_equalize(downstream.compiled_query)==query_equalize(f"""
SELECT 
    * 
FROM 
{downstream.quoted_dot_notation}
WHERE id IN (1,2,3) 
UNION
(SELECT
    *
FROM
{downstream.quoted_dot_notation}
WHERE
id 
NOT IN 
(SELECT
    id
FROM
{upstream.quoted_dot_notation})
LIMIT 100) 
""")

    assert query_equalize(upstream.compiled_query)==query_equalize(f"""
WITH {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS ( 
SELECT * FROM 
{upstream.quoted_dot_notation} 
    WHERE id in (SELECT id 
       FROM 
{downstream.quoted_dot_notation}) 
)
,{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS ( 
SELECT 
    * 
FROM 
    {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} SAMPLE BERNOULLI (1500 ROWS)
) 
SELECT 
    * 
FROM 
    {relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} 
UNION 
(SELECT 
    * 
FROM 
{upstream.quoted_dot_notation} 
WHERE 
    id 
NOT IN 
    (SELECT 
        id 
    FROM 
{downstream.quoted_dot_notation}) LIMIT 100)
"""
)

def test_run_deps_bidirectional_exclude_outliers(stub_relation_set):
    upstream=stub_relation_set.upstream_relation
    downstream=stub_relation_set.downstream_relation
    upstream.data=pd.DataFrame([dict(id=1),dict(id=2),dict(id=3)])
    for relation in (downstream,upstream,):
        relation.attributes=[Attribute('id',dt.INTEGER)]
        relation.sample_method=BernoulliSample(10)
        relation.sampling=DefaultSampling()
        relation.sampling.sample_method._rows == 1500
        relation.sampling.sample_method._units == 'rows'

    dag=nx.DiGraph()
    dag.add_edge(upstream,downstream,direction="bidirectional",remote_attribute='id',local_attribute='id')
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(upstream,dag,adapter,False)
    compiler.compile_queries_for_relation(downstream,dag,adapter,False)
    assert query_equalize(downstream.compiled_query)==query_equalize(f"""
SELECT 
    * 
FROM 
{downstream.quoted_dot_notation}
WHERE id IN (1,2,3) 
""")

    assert query_equalize(upstream.compiled_query)==query_equalize(f"""
WITH {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS ( 
SELECT 
    * 
FROM 
    {upstream.quoted_dot_notation} 
WHERE 
    id 
in (SELECT 
        id 
    FROM 
        {downstream.quoted_dot_notation}) ) 
,{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS ( 
    SELECT 
        * 
    FROM 
        {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} SAMPLE BERNOULLI (1500 ROWS) 
) 
SELECT 
    * 
FROM 
{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')}
""")
