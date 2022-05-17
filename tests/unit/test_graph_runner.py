import copy
from unittest import mock
from unittest.mock import ANY

import pandas as pd

from snowshu.core.graph_set_runner import GraphExecutable, GraphSetRunner
from snowshu.samplings.samplings import DefaultSampling


def test_traverse_and_execute_analyze(stub_graph_set):
    source_adapter,target_adapter=[mock.MagicMock() for _ in range(2)]
    source_adapter.predicate_constraint_statement.return_value=str()
    source_adapter.upstream_constraint_statement.return_value=str()
    source_adapter.union_constraint_statement.return_value=str()
    source_adapter.sample_statement_from_relation.return_value=str()
    runner=GraphSetRunner()
    runner.barf=False
    graph_set,vals=stub_graph_set
    source_adapter.scalar_query.return_value=1000
    source_adapter.check_count_and_query.return_value=pd.DataFrame([dict(population_size=1000,sample_size=100)])
    dag=copy.deepcopy(graph_set[-1]) # last graph in the set is the dag
    
    ## stub in the sampling pop defaults
    for rel in dag.nodes:
        rel.unsampled=False
        rel.include_outliers=False
        rel.sampling=DefaultSampling()

    dag_executable = GraphExecutable(dag, source_adapter, target_adapter, True)

    # longer dag
    runner._traverse_and_execute(dag_executable)
    for rel in dag.nodes:
        assert not isinstance(getattr(rel, 'data', None), pd.DataFrame)
        assert rel.source_extracted is True
        assert rel.target_loaded is False
        assert rel.sample_size == 100
        assert rel.population_size == 1000

    # iso dag
    iso = copy.deepcopy(graph_set[0])  # first graph in the set is an iso
    [node for node in iso.nodes][0].sampling = DefaultSampling()
    [node for node in iso.nodes][0].unsampled=False
    [node for node in iso.nodes][0].include_outliers=False


    iso_executable = GraphExecutable(iso, source_adapter, target_adapter, True)
    assert not isinstance(
        getattr(vals.iso_relation, 'data', None), pd.DataFrame)
    runner._traverse_and_execute(iso_executable)
    iso_relation = [node for node in iso.nodes][0]
    assert iso_relation.source_extracted is True
    assert iso_relation.target_loaded is False
    assert iso_relation.sample_size == 100
    assert iso_relation.population_size == 1000


def test_traverse_and_execute_custom_max_rows_pass(stub_graph_set):
    """
    Tests if the values stated in config are correctly passed to check_count_and_query() method
    """

    source_adapter,target_adapter=[mock.MagicMock() for _ in range(2)]
    source_adapter.predicate_constraint_statement.return_value=str()
    source_adapter.upstream_constraint_statement.return_value=str()
    source_adapter.union_constraint_statement.return_value=str()
    source_adapter.sample_statement_from_relation.return_value=str()
    runner=GraphSetRunner()
    runner.barf=False
    graph_set,vals=stub_graph_set
    source_adapter.scalar_query.return_value=1000
    source_adapter.check_count_and_query.return_value=pd.DataFrame([dict(population_size=1000,sample_size=100)])
    dag=copy.deepcopy(graph_set[-1]) # last graph in the set is the dag
    
    # test if defaults are passed
    for rel in dag.nodes:
        rel.unsampled=False
        rel.include_outliers=False
        rel.sampling=DefaultSampling()

    dag_executable = GraphExecutable(dag, source_adapter, target_adapter, True)

    with mock.patch.object(source_adapter, 'check_count_and_query') as mock_1:
        runner._traverse_and_execute(dag_executable)
        mock_1.assert_called_with(ANY, 1000000, ANY)

    # test if custom values are passed
    for rel in dag.nodes:
        rel.unsampled=False
        rel.include_outliers=False
        rel.sampling=DefaultSampling()
        rel.sampling.max_allowed_rows = 1234567

    dag_executable = GraphExecutable(dag, source_adapter, target_adapter, True)

    with mock.patch.object(source_adapter, 'check_count_and_query') as mock_1:
        runner._traverse_and_execute(dag_executable)
        mock_1.assert_called_with(ANY, 1234567, ANY)