import pytest
from tests.common import rand_string
from io import StringIO
import yaml
from snowshu.core.configuration_parser import ConfigurationParser
import os


def test_fills_in_empty_source_values(stub_replica_configuration):

    for rel in stub_replica_configuration.specified_relations:
        assert isinstance(rel.unsampled, bool)
        assert isinstance(rel.relationships.bidirectional, list)

        for direction in ('bidirectional', 'directional',):
            assert getattr(rel.relationships, direction) is not None


def test_fills_empty_top_level_values(stub_configs):
    stub_configs = stub_configs()
    del stub_configs['long_description']
    mock_config_file = StringIO(yaml.dump(stub_configs))
    parsed = ConfigurationParser.from_file_or_path(mock_config_file)

    assert parsed.long_description == ''


def test_errors_on_missing_section(stub_configs):
    stub_configs = stub_configs()
    del stub_configs['source']
    with pytest.raises(AttributeError):
        mock_config_file = StringIO(yaml.dump(stub_configs))
        ConfigurationParser.from_file_or_path(mock_config_file)
