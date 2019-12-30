import pytest
import mock
import pandas as pd
from snowshu.core.graph_set_runner import GraphSetRunner, GraphExecutable
from snowshu.logger import Logger
from time import time
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
    runner._traverse_and_execute(dag_executable,time())
    assert vals.birelation_left.source_extracted is True
    assert vals.birelation_left.target_loaded is False
    assert vals.birelation_left.sample_size == 100 
    assert vals.birelation_left.population_size == 1000 

    ## iso dag
    iso=graph_set[0] # first graph in the set is an iso
    iso_executable=GraphExecutable(iso, source_adapter,target_adapter,True)
    assert not isinstance(getattr(vals.iso_relation,'data',None),pd.DataFrame)
    runner._traverse_and_execute(iso_executable,time())
    assert vals.iso_relation.source_extracted is True
    assert vals.iso_relation.target_loaded is False
    assert vals.iso_relation.sample_size == 100 
    assert vals.iso_relation.population_size == 1000 



##TODO this will need more stubbing for run tests
@pytest.mark.skip
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
