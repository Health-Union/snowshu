import threading
import pytest
import json
import yaml
import os

from snowshu.adapters.target_adapters.snowflake_adapter.snowflake_adapter import (
    SnowflakeAdapter,
)
from snowshu.configs import PACKAGE_ROOT


CONFIGURATION_PATH = os.path.join(
    PACKAGE_ROOT, "tests", "assets", "replica_test_config_snowflake.yml"
)

DB_LOCK = threading.Lock()
DATABASES = set()

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


def test_create_database_if_not_exists(
    sf_adapter, db_lock=DB_LOCK, databases=DATABASES
):
    sf_adapter.create_database_if_not_exists(
        "test_database", uuid="1234", db_lock=db_lock, databases=databases
    )
    assert sf_adapter.conn.execute(
        "SHOW DATABASES LIKE 'SNOWSHU_1234_INTEGRATION_TEST_TEST_DATABASE'"
    ).fetchone()
    sf_adapter.conn.execute(
        "DROP DATABASE IF EXISTS SNOWSHU_1234_INTEGRATION_TEST_TEST_DATABASE"
    )


def test_rolling_back_database_creation(
    sf_adapter, db_lock=DB_LOCK, databases=DATABASES
):
    sf_adapter.create_database_if_not_exists(
        "test_database", uuid="1234", db_lock=db_lock, databases=databases
    )
    assert sf_adapter.conn.execute(
        "SHOW DATABASES LIKE 'SNOWSHU_1234_INTEGRATION_TEST_TEST_DATABASE'"
    ).fetchone()
    sf_adapter.rollback_database_creation(databases=databases)
    assert not sf_adapter.conn.execute(
        "SHOW DATABASES LIKE 'SNOWSHU_1234_INTEGRATION_TEST_TEST_DATABASE'"
    ).fetchone()

    
def test_create_schema_if_not_exists(sf_adapter, db_lock=DB_LOCK, databases=DATABASES):
    sf_adapter.create_database_if_not_exists(
        "test_database", uuid="1234", db_lock=db_lock, databases=databases
    )
    sf_adapter.create_schema_if_not_exists(
        "test_database", "test_schema", uuid="1234"
    )
    result = sf_adapter.conn.execute(
        "SELECT SCHEMA_NAME FROM SNOWSHU_1234_INTEGRATION_TEST_TEST_DATABASE.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'TEST_SCHEMA'"
    ).fetchone()
    assert result is not None, "Schema does not exist"
    sf_adapter.conn.execute(
        "DROP DATABASE IF EXISTS SNOWSHU_1234_INTEGRATION_TEST_TEST_DATABASE CASCADE"
    )