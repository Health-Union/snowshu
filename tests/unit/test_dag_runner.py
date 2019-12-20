import pytest
import mock
import pandas as pd
from snowshu.core.graph_set_runner import GraphSetRunner, GraphExecutable
from snowshu.logger import Logger
log_engine=Logger()
#log_engine.set_log_level(10)
logger=log_engine.logger


def test_traverse_and_execute_analyze(stub_graph_set):
    source_adapter,target_adapter=[mock.MagicMock() for _ in range(2)]
    runner=GraphSetRunner()
    graph_set,vals=stub_graph_set
    
    source_adapter.check_count_and_query.return_value=pd.DataFrame([dict(population_size=1000,sample_size=100)])
    dag=graph_set[-1] # last graph in the set is the dag
    dag_executable=GraphExecutable(dag, source_adapter,target_adapter,True)

    ## longer dag
    assert not isinstance(getattr(vals.birelation_left,'data',None),pd.DataFrame)
    runner._traverse_and_execute(dag_executable)
    assert isinstance(vals.birelation_left.data,pd.DataFrame)


    ## iso dag
    iso=graph_set[0] # first graph in the set is an iso
    iso_executable=GraphExecutable(iso, source_adapter,target_adapter,True)
    assert not isinstance(getattr(vals.iso_relation,'data',None),pd.DataFrame)
    runner._traverse_and_execute(iso_executable)
    assert isinstance(vals.iso_relation.data,pd.DataFrame)

def test_executes_dags(stub_graph_set):
    source_adapter,target_adapter=[mock.MagicMock() for _ in range(2)]
    graph_set, vals=stub_graph_set
    source_adapter.check_count_and_query.return_value=pd.DataFrame([dict(sample_size=100,population_size=1000)])
    runner=GraphSetRunner()
    runner.execute_graph_set(graph_set,source_adapter,target_adapter,threads=4,analyze=True)

    source_adapter.check_count_and_query.assert_called

    for graph in graph_set:
        for relation,data in graph.nodes.items():
            assert isinstance(relation.data,pd.DataFrame)
