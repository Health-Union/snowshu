import pytest
import json
import yaml
import os
from unittest import mock

from snowshu.adapters.target_adapters.snowflake_adapter.snowflake_adapter import (
    SnowflakeAdapter,
)
from snowshu.configs import PACKAGE_ROOT
from snowshu.core.models.relation import Relation

CONFIGURATION_PATH = os.path.join(
    PACKAGE_ROOT, "tests", "assets", "replica_test_config_snowflake.yml"
)

@pytest.fixture(scope="session")
def sf_adapter():

    with open(CONFIGURATION_PATH) as config_file:
        full_config = yaml.safe_load(config_file)

    adapter_args = full_config["target"].get("adapter_args")
    if not adapter_args:
        adapter_args = {}
    metadata = {
        attr: full_config[attr]
        for attr in (
            "name",
            "short_description",
            "long_description",
        )
    }
    metadata["config_json"] = json.dumps(full_config)
    adapter_args["replica_metadata"] = metadata
    
    adapter = SnowflakeAdapter(**adapter_args)
    return adapter

def test_clone_relation(mocker, sf_adapter):
    mock_safe_query = mocker.patch.object(sf_adapter, "_safe_query")
    
    relation = Relation(
        name="target_table",
        schema="target_schema",
        database="target_database",
        materialization=mock.MagicMock(),
        attributes=[],
    )
    relation.temp_schema = "source_schema"
    relation.temp_database = "source_database"

    sf_adapter.clone_relation(relation)
    
    # Check if _safe_query was called
    assert mock_safe_query.called, "_safe_query was not called"
    
    actual_query = mock_safe_query.call_args[0][0].strip()
    
    # Extract the prefix from the actual query
    prefix = actual_query.split(' ')[5].split('_target_database')[0]
    
    expected_query = f"""
        CREATE TABLE IF NOT EXISTS {prefix}_target_database.target_schema.target_table AS
        SELECT * FROM source_database.source_schema.target_table
    """.strip()
    
    # Normalize whitespace
    expected_query = ' '.join(expected_query.split())
    actual_query = ' '.join(actual_query.split())
    
    assert actual_query == expected_query, f"Expected: {expected_query}, Actual: {actual_query}"