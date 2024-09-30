import json
import pytest
from unittest import mock

from snowshu.adapters.target_adapters.snowflake_adapter.snowflake_adapter import (
    SnowflakeAdapter,
)
from snowshu.core.models import Relation, materializations as mz
from snowshu.core.models.materializations import TABLE, VIEW
from snowshu.core.models.credentials import Credentials


@pytest.fixture
def mock_adapter():
    config = {"credpath": "tests/assets/integration/credentials_snowflake_target.yml"}
    replica_metadata = {"name": "test_replica", "config_json": json.dumps(config)}
    adapter = SnowflakeAdapter(replica_metadata=replica_metadata)

    with mock.patch.object(adapter, "_generate_credentials") as mock_generate_creds:
        mock_generate_creds.return_value = Credentials(
            user="mock_user",
            password="mock_password",
            account="mock_account",
            database="mock_database",
            role="mock_role",
        )
        adapter.credentials = mock_generate_creds.return_value
        adapter.conn = mock.MagicMock()  # Mock the connection if needed
        yield adapter


def test_get_relations_from_database_success(mock_adapter):
    # Arrange
    database = "test_db"
    schema = "public"

    # Mock the _safe_query method to return a predefined DataFrame
    mock_data = mock.Mock()
    mock_data.iterrows.return_value = [
        (0, {"relation": "users", "schema": "public", "materialization": "BASE TABLE"}),
        (1, {"relation": "orders", "schema": "public", "materialization": "VIEW"}),
        (
            2,
            {"relation": "users", "schema": "public", "materialization": "BASE TABLE"},
        ),  # Duplicate relation
    ]

    with mock.patch.object(SnowflakeAdapter, "_safe_query", return_value=mock_data):
        with mock.patch.object(mock_adapter, "quoted", side_effect=lambda x: x):
            # Act
            relations = mock_adapter._get_relations_from_database(database, schema)

    # Assert
    assert len(relations) == 2
    assert any(
        rel.name == "users" and rel.materialization == TABLE for rel in relations
    )
    assert any(
        rel.name == "orders" and rel.materialization == VIEW for rel in relations
    )


def test_build_catalog_success(mock_adapter):
    # Arrange
    replica_prefix = "SNOWSHU_12345_TEST_REPLICA"
    mock_adapter.replica_prefix = replica_prefix
    databases = [
        "SNOWSHU_12345_TEST_REPLICA_DB1",
        "SNOWSHU_12345_TEST_REPLICA_DB2",
        "OTHER_DB",  # This should be skipped
    ]

    schemas_mapping = {
        "SNOWSHU_12345_TEST_REPLICA_DB1": ["schema1", "schema2"],
        "SNOWSHU_12345_TEST_REPLICA_DB2": ["schema3"],
    }

    relations_mapping = {
        ("SNOWSHU_12345_TEST_REPLICA_DB1", "schema1"): [
            Relation(
                database="DB1",
                schema="schema1",
                name="table1",
                materialization=mz.TABLE,
                attributes=[],
            )
        ],
        ("SNOWSHU_12345_TEST_REPLICA_DB1", "schema2"): [
            Relation(
                database="DB1",
                schema="schema2",
                name="view1",
                materialization=mz.VIEW,
                attributes=[],
            )
        ],
        ("SNOWSHU_12345_TEST_REPLICA_DB2", "schema3"): [
            Relation(
                database="DB2",
                schema="schema3",
                name="table2",
                materialization=mz.TABLE,
                attributes=[],
            )
        ],
    }

    with mock.patch.object(
        SnowflakeAdapter, "_get_all_databases", return_value=databases
    ), mock.patch.object(
        SnowflakeAdapter,
        "_get_all_schemas",
        side_effect=lambda db: schemas_mapping.get(db, []),
    ), mock.patch.object(
        SnowflakeAdapter,
        "_get_relations_from_database",
        side_effect=lambda db, schema: relations_mapping.get((db, schema), []),
    ), mock.patch.object(mock_adapter, "quoted", side_effect=lambda x: x):
        # Act
        catalog = mock_adapter.build_catalog(thread_workers=2)

    # Assert
    expected_catalog = {
        relations_mapping[("SNOWSHU_12345_TEST_REPLICA_DB1", "schema1")][0],
        relations_mapping[("SNOWSHU_12345_TEST_REPLICA_DB1", "schema2")][0],
        relations_mapping[("SNOWSHU_12345_TEST_REPLICA_DB2", "schema3")][0],
    }
    assert catalog == expected_catalog

