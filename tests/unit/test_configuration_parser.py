import json
import tempfile
from io import StringIO
from pathlib import Path
import copy

import pytest
import yaml
from jsonschema.exceptions import ValidationError

from snowshu.configs import DEFAULT_MAX_NUMBER_OF_OUTLIERS
from snowshu.core.configuration_parser import ConfigurationParser, REPLICA_JSON_SCHEMA, CREDENTIALS_JSON_SCHEMA, materializations
from snowshu.samplings.samplings import DefaultSampling
from tests.common import rand_string


def test_fills_in_empty_source_values(stub_replica_configuration):
    for rel in stub_replica_configuration.specified_relations:
        assert isinstance(rel.unsampled, bool)
        assert isinstance(rel.relationships.directional, list)
        assert isinstance(rel.relationships.bidirectional, list)
        assert isinstance(rel.relationships.polymorphic, list)

        for direction in ('bidirectional', 'directional', 'polymorphic'):
            assert getattr(rel.relationships, direction) is not None


def test_fills_empty_top_level_values(stub_configs):
    stub_configs = stub_configs()
    del stub_configs['long_description']
    for attr in ('include_outliers', 'max_number_of_outliers',):
        if attr in stub_configs['source'].keys():
            del stub_configs['source'][attr]
    mock_config_file = StringIO(yaml.dump(stub_configs))
    parsed = ConfigurationParser().from_file_or_path(mock_config_file)

    assert parsed.long_description == ''
    assert parsed.include_outliers == False
    assert parsed.max_number_of_outliers == DEFAULT_MAX_NUMBER_OF_OUTLIERS


def test_casing_polymorphic_overrides(stub_configs):
    stub_configs = stub_configs()
    mock_config_file = StringIO(yaml.dump(stub_configs))
    parsed = ConfigurationParser().from_file_or_path(mock_config_file)
    override_relation = [
        rel for rel in parsed.specified_relations if rel.relation_pattern == 'PARENT_TABLE'][0]
    overrides = override_relation.relationships.polymorphic[0].local_type_overrides
    assert overrides
    assert 'SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.CHILD_TYPE_2_ITEMS' in overrides
    assert overrides['SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.CHILD_TYPE_2_ITEMS'] == 'type_2'


def test_errors_on_missing_section(stub_configs):
    stub_configs = stub_configs()
    del stub_configs['source']
    with pytest.raises((KeyError, AttributeError,)):
        mock_config_file = StringIO(yaml.dump(stub_configs))
        ConfigurationParser().from_file_or_path(mock_config_file)


def test_sets_sampling_for_all_patterns(stub_configs):
    stub_configs = stub_configs()
    mock_config_file = StringIO(yaml.dump(stub_configs))
    parsed = ConfigurationParser().from_file_or_path(mock_config_file)

    assert isinstance(parsed.sampling, DefaultSampling)


def test_errors_on_bad_profile(stub_configs):
    stub_configs = stub_configs()
    SOURCE_PROFILE, TARGET_PROFILE, STORAGE_PROFILE = [
        rand_string(10) for _ in range(3)]
    stub_configs['source']['profile'] = SOURCE_PROFILE
    stub_configs['storage']['profile'] = STORAGE_PROFILE

    with pytest.raises(ValueError):
        mock_config_file = StringIO(yaml.dump(stub_configs))
        ConfigurationParser().from_file_or_path(mock_config_file)


def test_loads_good_creds(stub_creds, stub_configs):
    stub_creds = stub_creds()
    stub_configs = stub_configs()

    SOURCES_NAME, SOURCES_PASSWORD, STORAGES_ACCOUNT = [
        rand_string(10) for _ in range(3)]
    with tempfile.NamedTemporaryFile(mode='w') as mock_file:
        stub_creds['sources'][0]['name'] = SOURCES_NAME
        stub_creds['sources'][0]['password'] = SOURCES_PASSWORD
        stub_configs['source']['profile'] = SOURCES_NAME
        json.dump(stub_creds, mock_file)
        mock_file.seek(0)
        stub_configs['credpath'] = mock_file.name
        adapter_profile = ConfigurationParser()._build_adapter_profile('source', stub_configs)

    assert adapter_profile.name == SOURCES_NAME
    assert adapter_profile.adapter.credentials.password == SOURCES_PASSWORD


def test_schema_verification_errors(stub_creds, stub_configs):
    stub_creds = stub_creds()
    stub_configs = stub_configs()
    # create type error in replica.yml
    stub_creds['sources'][0]['password'] = True

    with tempfile.NamedTemporaryFile(mode='w') as mock_file:
        json.dump(stub_creds, mock_file)
        mock_file.seek(0)
        stub_configs['credpath'] = mock_file.name
        with pytest.raises(ValidationError) as exc:
            ConfigurationParser()._build_adapter_profile('source', stub_configs)
    assert "True is not of type 'string'" in str(exc.value)

    # config with missing credentials file
    with pytest.raises(FileNotFoundError) as fnf_err:
        mock_config_file = StringIO(yaml.dump(stub_configs))
        ConfigurationParser().from_file_or_path(mock_config_file)
    assert "Credentials specified in replica.yml not found" in fnf_err.value.strerror


def test_schema_verification(tmpdir, stub_creds, stub_configs):
    """ Verifies that the configuration parser can load a proper file. """
    replica_file = Path(tmpdir / 'replica_file.yml')
    cred_file = Path(tmpdir / 'credentials_file.yml')
    stub_creds = stub_creds()
    stub_configs = stub_configs()
    stub_configs["credpath"] = str(cred_file.absolute())

    cred_file.write_text(json.dumps(stub_creds))
    replica_file.write_text(json.dumps(stub_configs))

    replica_config = ConfigurationParser()._get_dict_from_anything(
        replica_file, REPLICA_JSON_SCHEMA)
    cred_config = ConfigurationParser()._get_dict_from_anything(
        cred_file, CREDENTIALS_JSON_SCHEMA)
    assert isinstance(replica_config, dict)
    assert isinstance(cred_config, dict)


materialization_mappings_test_cases = [
    (   
        True,  # set to True
        {
            "BASE TABLE": materializations.TABLE,
            "VIEW": materializations.TABLE
        }
    ),
    (
        False,  # set to False
        {
            "BASE TABLE": materializations.TABLE,
            "VIEW": materializations.VIEW
        }
    ),
    (
        None,  # Not set
        {
            "BASE TABLE": materializations.TABLE,
            "VIEW": materializations.TABLE
        }
    ),
]


@pytest.mark.parametrize('condition,expected', materialization_mappings_test_cases)
def test_materialization_mappings(condition, expected, stub_configs):
    local_stub_configs = stub_configs()
    if condition != None:
        local_stub_configs['source']['copy_views_as_tables'] = condition
    mock_config_file = StringIO(yaml.dump(local_stub_configs))
    parsed = ConfigurationParser().from_file_or_path(mock_config_file)

    assert parsed.source_profile.adapter.MATERIALIZATION_MAPPINGS == expected
