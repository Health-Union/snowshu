import mock
import json
from pathlib import Path
from tests.common import rand_string
from snowshu.configs import DOCKER_TARGET_CONTAINER
from snowshu.core.replica.replica_factory import ReplicaFactory
from tests.common import rand_string


@mock.patch('snowshu.core.replica.replica_factory.SnowShuGraph.build_graph')
@mock.patch('snowshu.core.replica.replica_factory.SnowShuGraph.get_graphs',return_value=[])
def tests_replica_rename(get_graphs, build_graph, stub_configs):
    replica=ReplicaFactory()
    replica.load_config(stub_configs())
    test_name=rand_string(10)
    replica.create(test_name, False)
    assert build_graph.call_args[0][0].name == test_name


def test_loading_specified_replica_file(tmpdir, stub_creds, stub_configs):
    """ Verify that a different named config file is loaded properly. """
    replica_file = Path(tmpdir / 'my_custom_name.yml')
    cred_file = Path(tmpdir / 'my_cred_file.yml')
    stub_creds = stub_creds()
    stub_configs = stub_configs()
    stub_configs["credpath"] = str(cred_file.absolute())

    cred_file.write_text(json.dumps(stub_creds))
    replica_file.write_text(json.dumps(stub_configs))

    replica = ReplicaFactory()
    assert not replica.config  # no config in new obj
    replica.load_config(replica_file)
    assert replica.config  # config should now be loaded