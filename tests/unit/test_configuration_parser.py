import pytest
from tests.common import rand_string
from io import StringIO
import yaml
from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.utils import PACKAGE_ROOT
import os

def test_fills_in_empty_source_values(stub_configs):
    stub_configs=stub_configs()
    mock_config_file=StringIO(yaml.dump(stub_configs))   
    config_object = ConfigurationParser.from_file_or_path(mock_config_file)
    
    for rel in config_object.specified_relations:
        assert isinstance(rel.unsampled,bool)
        assert isinstance(rel.relationships,dict)

        for direction in ('bidirectional','directional',):
            assert direction in rel.relationships.keys()

@pytest.mark.skip
def test_fills_empty_top_level_values(stub_configs):
    parser=ConfigurationParser()
    stub_configs=stub_configs()
    del stub_configs['long_description']
    mock_config_file=StringIO(yaml.dump(stub_configs))   
    parser.from_file_or_path(mock_config_file)

    assert parser.long_description == ''

@pytest.mark.skip
def test_errors_on_missing_section(stub_configs):
    SOURCE_PROFILE,TARGET_PROFILE,STORAGE_PROFILE=[rand_string(10) for _ in range(3)]
    stub_configs=stub_configs()
    del stub_configs['source']
    with pytest.raises(AttributeError):
        parser=ConfigurationParser()
        mock_config_file=StringIO(yaml.dump(stub_configs))   
        parser.from_file_or_path(mock_config_file)


