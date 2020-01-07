from dataclasses import dataclass
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
from snowshu.core.compile import BaseCompiler
from snowshu.adapters.source_adapters.sample_methods import BernoulliSample
from snowshu.core.models.attribute import Attribute
import snowshu.core.models.data_types as dt


def tests_analyze_unsampled():
    pass









@pytest.fixture
def control_dag():
    dag=nx.DiGraph()
    DATABASE,SCHEMA,TABLE1,TABLE2,TABLE3,TABLE4,FKEY=[rand_string(10) for _ in range(7)]
    fkey_attribute=Attribute(FKEY,dt.INTEGER)
    base_vals=dict(database=DATABASE,schema=SCHEMA,materialization=mz.TABLE,attributes=[fkey_attribute])    
    
    class TestData:
        
        def __init__(self,base_vals,database,schema,table1,table2,table3,table4,fkey):
            self.ISO_RELATION=Relation(**base_vals,name=table1)
            self.DEPENDENT_RELATION=Relation(**base_vals,name=table2)
            self.DEPENDING_RELATION=Relation(**base_vals,name=table3)
            self.DIRECTIONAL_RELATION=Relation(**base_vals,name=table4)
            self.DATABASE=DATABASE
            self.SCHEMA=SCHEMA
            self.TABLE1=table1
            self.TABLE2=table2
            self.TABLE3=table3
            self.TABLE4=table4
            self.FKEY=fkey            



    td=TestData(base_vals,DATABASE,SCHEMA,TABLE1,TABLE2,TABLE3,TABLE4,FKEY)
    dag.add_node(td.ISO_RELATION)
    dag.add_edge(td.DEPENDENT_RELATION,td.DEPENDING_RELATION, direction="bidirectional",local_attribute=FKEY,remote_attribute=FKEY)
    dag.add_edge(td.DEPENDENT_RELATION,td.DIRECTIONAL_RELATION, direction="directional",local_attribute=FKEY,remote_attribute=FKEY)
    return dag ,td


def test_compiles_isolate_statement_analyze(control_dag):
    graph,relations=control_dag
    dag=[graph]
    for node in dag[0].nodes:
        node.compiled_query=''
    adapter=SnowflakeAdapter()
    comp=BaseCompiler(dag,adapter,sample_method=BernoulliSample(10), analyze=True)
    comp.compile()

    assert query_equalize(relations.ISO_RELATION.compiled_query)==query_equalize(f"""
WITH
    __SNOWSHU_COUNT_POPULATION AS (
SELECT
    COUNT(*) AS population_size
FROM
    "{relations.DATABASE}"."{relations.SCHEMA}"."{relations.TABLE1}"
)
,__SNOWSHU_CORE_SAMPLE AS (
SELECT
    *
FROM 
    "{relations.DATABASE}"."{relations.SCHEMA}"."{relations.TABLE1}"
    SAMPLE BERNOULLI (10)
)
,__SNOWSHU_CORE_SAMPLE_COUNT AS (
SELECT
    COUNT(*) AS sample_size
FROM
    __SNOWSHU_CORE_SAMPLE
)
SELECT
    s.sample_size AS sample_size
    ,p.population_size AS population_size
FROM
    __SNOWSHU_CORE_SAMPLE_COUNT s
INNER JOIN
    __SNOWSHU_COUNT_POPULATION p
ON
    1=1
LIMIT 1
""")

def test_compiles_isolate_statement(control_dag):
    graph,relations=control_dag
    dag=[graph]
    df=DFMock(columns={relations.FKEY:'integer'})
    df.count=1000; df.generate_dataframe()
    for node in dag[0].nodes:
        node.compiled_query=''
        node.data=df.dataframe
    adapter=SnowflakeAdapter()
    comp=BaseCompiler(dag,adapter,sample_method=BernoulliSample(10), analyze=False)
    comp.compile()

    assert query_equalize(relations.ISO_RELATION.compiled_query)==query_equalize(f"""
SELECT
    *
FROM 
    "{relations.DATABASE}"."{relations.SCHEMA}"."{relations.TABLE1}"
    SAMPLE BERNOULLI (10)
""")

def test_compiles_depending_directional_statement(control_dag):
    graph,relations=control_dag
    dag=[graph]
    df=DFMock(columns={relations.FKEY:'integer'})
    df.count=1000; df.generate_dataframe()
    for node in dag[0].nodes:
        node.compiled_query=''
        node.data=df.dataframe
    adapter=SnowflakeAdapter()
    comp=BaseCompiler(dag,adapter,sample_method=BernoulliSample(10), analyze=False)
    comp.compile()

    assert query_equalize(f"""
SELECT
    *
FROM 
    "{relations.DATABASE}"."{relations.SCHEMA}"."{relations.TABLE3}"
    WHERE
        {relations.FKEY} IN ( SELECT {relations.FKEY} FROM ( SELECT
""") in query_equalize(relations.DEPENDING_RELATION.compiled_query)  

def test_compiles_dependent_directional_statement(control_dag):
    graph,relations=control_dag
    dag=[graph]
    df=DFMock(columns={relations.FKEY:'integer'})
    df.count=1000; df.generate_dataframe()
    for node in dag[0].nodes:
        node.compiled_query=''
        node.data=df.dataframe
    adapter=SnowflakeAdapter()
    comp=BaseCompiler(dag,adapter,sample_method=BernoulliSample(10), analyze=False)
    comp.compile()

    assert query_equalize(f"""
SELECT
    *
FROM 
    "{relations.DATABASE}"."{relations.SCHEMA}"."{relations.TABLE4}"
    WHERE
""") in query_equalize(relations.DIRECTIONAL_RELATION.compiled_query)


def test_unsampled_analyze(stub_graph_set):
    source_adapter=SnowflakeAdapter()
    source_adapter.check_count_and_query = mock.MagicMock()
    source_adapter.check_count_and_query.return_value=pd.DataFrame([dict(population_size=1000,sample_size=1000)])
    graph_set,vals=stub_graph_set
    vals.birelation_left.unsampled=True
    graph=nx.DiGraph()
    graph.add_node(vals.birelation_left)
    comp=BaseCompiler([graph],source_adapter,sample_method=BernoulliSample(10), analyze=True)
    comp.compile()
    assert query_equalize(vals.birelation_left.core_query) == query_equalize(f"""SELECT
*
FROM
{vals.birelation_left.quoted_dot_notation}""")

    assert query_equalize(vals.birelation_left.compiled_query) == query_equalize(f"""
WITH __SNOWSHU_COUNT_POPULATION AS ( SELECT COUNT(*) AS population_size FROM "{vals.birelation_left.database}"."{vals.birelation_left.schema}"."birelation_left" ) ,__SNOWSHU_CORE_SAMPLE AS ( SELECT * FROM "{vals.birelation_left.database}"."{vals.birelation_left.schema}"."birelation_left" ) ,__SNOWSHU_CORE_SAMPLE_COUNT AS ( SELECT COUNT(*) AS sample_size FROM __SNOWSHU_CORE_SAMPLE ) SELECT s.sample_size AS sample_size ,p.population_size AS population_size FROM __SNOWSHU_CORE_SAMPLE_COUNT s INNER JOIN __SNOWSHU_COUNT_POPULATION p ON 1=1 LIMIT 1""")
