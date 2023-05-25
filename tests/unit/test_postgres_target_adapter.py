from unittest.mock import MagicMock, ANY

from pandas.core.frame import DataFrame

from snowshu.adapters.target_adapters.postgres_adapter import PostgresAdapter
from snowshu.configs import DOCKER_REMOUNT_DIRECTORY, DOCKER_REPLICA_MOUNT_FOLDER
from snowshu.core.models import data_types
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.materializations import TABLE
from snowshu.core.models.relation import Relation
from tests.common import rand_string


def test_x00_replacement():
    adapter = PostgresAdapter(replica_metadata={})
    id_col = "id"
    content_col = "content"
    normal_val = "normal_value"
    weird_value = "weird\x00value"
    custom_replacement = "__CUSTOM_VALUE__"

    cols = [
        Attribute(id_col, data_types.BIGINT),
        Attribute(content_col, data_types.VARCHAR)
    ]
    # test default replacement
    relation = Relation("db", "schema", "relation", TABLE, cols)
    relation.data = DataFrame({id_col: [1, 2], content_col: [normal_val, weird_value]})

    fixed_relation = adapter.replace_x00_values(relation)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 1, [content_col]] == normal_val)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 2, [content_col]] == "weirdvalue")

    # test custom replacement
    adapter = PostgresAdapter(replica_metadata={}, pg_0x00_replacement=custom_replacement)
    relation = Relation("db", "schema", "relation", TABLE, cols)
    relation.data = DataFrame({id_col: [1, 2], content_col: [normal_val, weird_value]})

    fixed_relation = adapter.replace_x00_values(relation)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 1, [content_col]] == normal_val)
    assert all(
        fixed_relation.data.loc[fixed_relation.data[id_col] == 2, [content_col]] == f"weird{custom_replacement}value")


def test_create_snowshu_schema_statement():
    pg_adapter = PostgresAdapter(replica_metadata={})

    assert pg_adapter._create_snowshu_schema_statement() == 'CREATE SCHEMA IF NOT EXISTS snowshu;'


def test_quoted():
    pg_adapter = PostgresAdapter(replica_metadata={})
    val = rand_string(10)
    assert val == pg_adapter.quoted(val)

    val = rand_string(5) + ' ' + rand_string(6)
    assert f'"{val}"' == pg_adapter.quoted(val)


def test_is_fdw_schema():
    pg_adapter = PostgresAdapter(replica_metadata={})
    schema = "DATASCIENCE_DEV"
    unique_databases = ["HU_DATA", "SNOWFLAKE_SAMPLE_DATA", "DEMO_DB", "UTIL_DB"]
    splitted = schema.split('__')

    assert pg_adapter.is_fdw_schema(schema, unique_databases) == (
            len(splitted) == 2 and splitted[0] in unique_databases)


def test_image_initialize_bash_commands():
    pg_adapter = PostgresAdapter(replica_metadata={})
    PRELOADED_PACKAGES = ['postgresql-plpython3-12']
    commands = [f'apt-get update && apt-get install -y {" ".join(PRELOADED_PACKAGES)}']

    assert pg_adapter.image_initialize_bash_commands().sort() == commands.sort()


def test_build_snowshu_envars():
    pg_adapter = PostgresAdapter(replica_metadata={})
    snowshu_envars = ['POSTGRES_USER=snowshu',
                      'POSTGRES_PASSWORD=snowshu',
                      'POSTGRES_DB=snowshu',
                      f'PGDATA=/{DOCKER_REMOUNT_DIRECTORY}']
    envars = [f"{envar}=snowshu" for envar in snowshu_envars]
    envars.append(f"PGDATA=/{DOCKER_REMOUNT_DIRECTORY}")

    assert set(envars) == set(pg_adapter._build_snowshu_envars(snowshu_envars))


def test_copy_replica_command():
    """Test whether copy replica command is correctly called"""
    pg_adapter = PostgresAdapter(replica_metadata={})
    pg_adapter.stop_postgres = MagicMock()
    # Skip copy if only one container
    pg_adapter.container = MagicMock()
    pg_adapter.copy_replica_data()
    pg_adapter.container.exec_run.assert_not_called()

    # Do copy if there are 2
    pg_adapter.passive_container = MagicMock()
    exec_return_value = MagicMock()
    exec_return_value.exit_code = 0
    pg_adapter.passive_container.exec_run = MagicMock(return_value=exec_return_value)
    pg_adapter.copy_replica_data()
    pg_adapter.container.exec_run.assert_called()
