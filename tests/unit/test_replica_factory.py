import copy
import json
from io import StringIO
from pathlib import Path
from unittest import mock

import yaml

from snowshu.adapters.target_adapters.base_local_target_adapter import BaseLocalTargetAdapter
from snowshu.core.models import Relation
from snowshu.core.models.relation import alter_relation_case
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.core.utils import get_multiarch_list
from snowshu.configs import LOCAL_ARCHITECTURE
from tests.common import rand_string
from tests.conftest import BASIC_CONFIGURATION


@mock.patch('snowshu.core.replica.replica_factory.SnowShuGraph.build_graph')
@mock.patch('snowshu.core.replica.replica_factory.SnowShuGraph.get_connected_subgraphs', return_value=[])
def tests_replica_rename(_, build_graph, stub_configs):
    replica = ReplicaFactory()
    replica.load_config(
        stub_configs()
    )
    test_name = rand_string(10)
    replica.create(test_name, False, 1)
    assert build_graph.call_args[0][0].name == test_name


@mock.patch("snowshu.core.replica.replica_factory.SnowShuGraph")
def tests_incremental_flag(graph, stub_configs):
    graph.return_value.graph = mock.Mock()
    replica = ReplicaFactory()
    replica.load_config(stub_configs())
    test_name = rand_string(10)
    replica.incremental = rand_string(10)
    adapter = replica.config.target_profile.adapter = mock.Mock(
        spec=BaseLocalTargetAdapter
    )
    adapter.build_catalog = mock.MagicMock(return_value=set())
    result = replica.create(test_name, False, 1)
    adapter.initialize_replica.assert_called_once_with(
        config=replica.config, incremental_image=replica.incremental
    )
    adapter.build_catalog.assert_called()
    assert "image up-to-date" in result


def tests_incremental_run_patched(stub_graph_set, stub_relation_set):
    _, vals = stub_graph_set

    common_relation = Relation(name=rand_string(10), **stub_relation_set.rand_relation_helper())
    source_catalog = [alter_relation_case(str.upper)(common_relation),
                      vals.downstream_relation,
                      vals.upstream_relation,
                      vals.birelation_right]
    target_catalog = {alter_relation_case(str.lower)(copy.deepcopy(common_relation)),
                      vals.downstream_relation,
                      vals.upstream_relation,
                      vals.birelation_left,
                      vals.birelation_right}

    replica = ReplicaFactory()
    config_dict = copy.deepcopy(BASIC_CONFIGURATION)
    config_dict["source"]["specified_relations"] = [
        {
            "database": vals.downstream_relation.database,
            "schema": vals.downstream_relation.schema,
            "relation": vals.downstream_relation.name,
            "relationships": {
                "directional": [
                    {
                        "local_attribute": vals.directional_key,
                        "database": ".*",
                        "schema": ".*",
                        "relation": ".*RELATION.*$",
                        "remote_attribute": vals.directional_key
                    }
                ]
            }
        }
    ]
    config = StringIO(yaml.dump(config_dict))
    replica.load_config(config)
    replica.config.source_profile.adapter.build_catalog = mock.Mock()
    replica.config.source_profile.adapter.build_catalog.return_value = source_catalog
    test_name = rand_string(10)
    replica.incremental = rand_string(10)
    adapter = replica.config.target_profile.adapter = mock.Mock(spec=BaseLocalTargetAdapter)
    adapter.DEFAULT_CASE = 'lower'
    adapter.build_catalog = mock.MagicMock(return_value=target_catalog)
    result = replica.create(test_name, False, 1)
    adapter.initialize_replica.assert_called_once_with(
        config=replica.config, incremental_image=replica.incremental
    )
    adapter.build_catalog.assert_called()
    assert 'image up-to-date' in result


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
