import pytest
import mock
import copy
import pandas as pd
from snowshu.core.sample_methods import BernoulliSample
from snowshu.core.graph_set_runner import GraphSetRunner, GraphExecutable
from snowshu.logger import Logger
from time import time
log_engine = Logger()
# log_engine.set_log_level(10)
logger = log_engine.logger


def test_traverse_and_execute_analyze(stub_graph_set):
    source_adapter,target_adapter=[mock.MagicMock() for _ in range(2)]
    source_adapter.predicate_constraint_statement.return_value=str()
    source_adapter.upstream_constraint_statement.return_value=str()
    source_adapter.union_constraint_statement.return_value=str()
    source_adapter.sample_statement_from_relation.return_value=str()
    runner=GraphSetRunner()
    graph_set,vals=stub_graph_set
    source_adapter.check_count_and_query.return_value=pd.DataFrame([dict(population_size=1000,sample_size=100)])
    dag=copy.deepcopy(graph_set[-1]) # last graph in the set is the dag
    
    ## stub in the sampling pop
    for rel in dag.nodes:
        rel.sample_method = BernoulliSample(10)

    dag_executable = GraphExecutable(dag, source_adapter, target_adapter, True)

    # longer dag
    runner._traverse_and_execute(dag_executable, time())
    for rel in dag.nodes:
        assert not isinstance(getattr(rel, 'data', None), pd.DataFrame)
        assert rel.source_extracted is True
        assert rel.target_loaded is False
        assert rel.sample_size == 100
        assert rel.population_size == 1000

    # iso dag
    iso = copy.deepcopy(graph_set[0])  # first graph in the set is an iso
    [node for node in iso.nodes][0].sample_method = BernoulliSample(10)
    iso_executable = GraphExecutable(iso, source_adapter, target_adapter, True)
    assert not isinstance(
        getattr(vals.iso_relation, 'data', None), pd.DataFrame)
    runner._traverse_and_execute(iso_executable, time())
    iso_relation = [node for node in iso.nodes][0]
    assert iso_relation.source_extracted is True
    assert iso_relation.target_loaded is False
    assert iso_relation.sample_size == 100
    assert iso_relation.population_size == 1000
