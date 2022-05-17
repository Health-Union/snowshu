import copy
from unittest import mock

import pandas as pd
import networkx as nx
from requests import patch

from snowshu.core.graph_set_runner import GraphExecutable, GraphSetRunner
from snowshu.samplings.samplings import DefaultSampling
from snowshu.core.replica.replica_factory import ReplicaFactory
from tests.conftest_modules.test_configuration import CONFIGURATION
from snowshu.core.graph import SnowShuGraph
from snowshu.core.compile import RuntimeSourceCompiler


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


def test_traverse_and_execute_custom_max_rows_pass():
    """
    Tests if the values stated in config are correctly passed to check_count_and_query() method
    """

    def mock_execute(self,
                 barf: bool = False,
                 name=None):
        graph = SnowShuGraph()

        graph.build_graph(self.config)

        graphs = graph.get_connected_subgraphs()

        executables = [GraphExecutable(graph,
                                       self.config.source_profile.adapter,
                                       self.config.target_profile.adapter,
                                       analyze=True) for graph in graphs]
        return executables


    with mock.patch.object(ReplicaFactory, '_execute', new=mock_execute):   
        config = CONFIGURATION
        replica = ReplicaFactory()
        replica.load_config(config)
        executables = replica._execute(False, 'name')

        sampling_passed_values = {}
        for executable in executables:
            for i, relation in enumerate(
                        nx.algorithms.dag.topological_sort(executable.graph)):

                sampling_passed_values[relation.dot_notation] = relation.sampling.max_allowed_rows
        assert sampling_passed_values['SNOWSHU_DEVELOPMENT.EXTERNAL_DATA.SOCIAL_USERS_IMPORT'] == 123456
