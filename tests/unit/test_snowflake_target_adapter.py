from unittest import mock

from snowshu.adapters.target_adapters.snowflake_adapter.snowflake_adapter import (
    SnowflakeAdapter,
)
from snowshu.core.models.relation import Relation

def test_clone_relation(mocker):
    sf_adapter = SnowflakeAdapter(replica_metadata={})
    mocker.patch.object(sf_adapter, "create_database_name", return_value="test_database")
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
    
    assert mock_safe_query.call_count == 1, f"Expected 1 call, got {mock_safe_query.call_count}"
    
    expected_query = """
        CREATE TABLE IF NOT EXISTS test_database.target_schema.target_table AS
        SELECT * FROM source_database.source_schema.target_table
    """.strip()
    actual_query = mock_safe_query.call_args[0][0].strip()
    
    # Normalize whitespace
    expected_query = ' '.join(expected_query.split())
    actual_query = ' '.join(actual_query.split())
    
    assert actual_query == expected_query, f"Expected: {expected_query}, Actual: {actual_query}"