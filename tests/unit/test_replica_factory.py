import pytest
import mock
from tests.common import rand_string
from snowshu.core.replica.replica_factory import ReplicaFactory

@mock.patch('snowshu.core.replica.replica_factory.Catalog')
@mock.patch('snowshu.core.replica.replica_factory.SnowShuGraph.build_graph')
@mock.patch('snowshu.core.replica.replica_factory.SnowShuGraph.get_graphs',return_value=[])
def tests_replica_rename(_,build_graph,get_graphs,stub_configs):
    replica=ReplicaFactory()
    replica.load_config(stub_configs())
    test_name=rand_string(10)
    replica.create(test_name,False)
    assert build_graph.call_args[0][0].name == test_name
