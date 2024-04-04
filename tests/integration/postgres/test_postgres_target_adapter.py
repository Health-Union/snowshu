import time
from unittest import mock
import pytest

from pandas.core.frame import DataFrame
from sqlalchemy import create_engine
import docker

from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.adapters.target_adapters.postgres_adapter import PostgresAdapter
from snowshu.configs import (DOCKER_REMOUNT_DIRECTORY, DOCKER_REPLICA_MOUNT_FOLDER, LOCAL_ARCHITECTURE)
from snowshu.core.docker import SnowShuDocker
from snowshu.core.models import Relation, Attribute, data_types
from snowshu.core.models.materializations import TABLE
from tests.common import rand_string
from tests.integration.snowflake.test_end_to_end import DOCKER_SPIN_UP_TIMEOUT

TEST_NAME, TEST_TABLE = [rand_string(10) for _ in range(2)]


def test_create_database_if_not_exists(end_to_end):
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    DATABASE = rand_string(10)
    pg_adapter.create_database_if_not_exists(DATABASE)
    database_list = pg_adapter._get_all_databases()

    assert DATABASE in database_list


def test_create_schema_if_not_exists(end_to_end):
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    DATABASE, SCHEMA = [rand_string(10) for _ in range(2)]
    pg_adapter.create_database_if_not_exists(DATABASE)
    pg_adapter.create_schema_if_not_exists(DATABASE, SCHEMA)
    list = pg_adapter._get_all_schemas(DATABASE)

    assert SCHEMA in list


def test_get_all_databases(end_to_end):
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    db_list = pg_adapter._get_all_databases()
    databases = ['postgres', 'snowshu', 'snowshu_development']

    assert set(databases).issubset(db_list)


def test_get_all_schemas(end_to_end):
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    schemas_list = pg_adapter._get_all_schemas('snowshu_development')
    schemas_set = {'polymorphic_data', 'external_data', 'tests_data', 'source_system'}

    assert schemas_set.issubset(schemas_list)


def test_get_relations_from_database(end_to_end):
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    SCHEMA_OBJ = BaseTargetAdapter._DatabaseObject("snowshu",
                                                   Relation("snowshu", "snowshu", "replica_meta", "", None))
    relations_list = [Relation("snowshu", "snowshu", "replica_meta", TABLE, None)]
    received_relations_list = pg_adapter._get_relations_from_database(schema_obj=SCHEMA_OBJ)

    assert set(relations_list).issubset(received_relations_list)


def test_create_all_database_extensions(end_to_end):
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    extensions = 'citext'
    pg_adapter.create_all_database_extensions()
    statement = 'SELECT extname FROM pg_extension'
    conn = pg_adapter.get_connection()
    result = conn.execute(statement).fetchall()
    extensions_list = [r[0] for r in result]

    assert extensions in extensions_list


def test_load_data_into_relation_relation(end_to_end):
    """Tests that data is loaded into a relation from relation.data"""
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    id_column = "id"
    content_column = "content"
    columns = [
        Attribute(id_column, data_types.BIGINT),
        Attribute(content_column, data_types.VARCHAR)
    ]
    relation = Relation("snowshu", "snowshu", "replica_meta", TABLE, columns)
    relation.data = DataFrame({id_column: [1, 2, 3], content_column: [rand_string(5) for _ in range(3)]})
    pg_adapter.load_data_into_relation(relation)

    statement = "SELECT * FROM snowshu.snowshu.replica_meta"
    conn = pg_adapter.get_connection()
    result = conn.execute(statement).fetchall()

    assert len(result) == 3


def test_load_data_into_relation_dataframe(end_to_end):
    """Tests that data is loaded into a relation from local dataframe"""
    pg_adapter = PostgresAdapter(replica_metadata={})
    if pg_adapter.target != "localhost":
        pg_adapter._credentials.host = 'integration-test'

    id_column = "id"
    content_column = "content"
    columns = [
        Attribute(id_column, data_types.BIGINT),
        Attribute(content_column, data_types.VARCHAR)
    ]
    relation = Relation("snowshu", "snowshu", "replica_meta", TABLE, columns)
    query_data = DataFrame({id_column: [1, 2, 3], content_column: [rand_string(5) for _ in range(3)]})
    pg_adapter.load_data_into_relation(relation, query_data)

    statement = "SELECT * FROM snowshu.snowshu.replica_meta"
    conn = pg_adapter.get_connection()
    result = conn.execute(statement).fetchall()

    assert len(result) == 3


@mock.patch('snowshu.core.docker.DOCKER_REPLICA_VOLUME', 'snowshu_container_share_validations')
def test_restore_data_from_shared_replica(docker_flush):
    shdocker = SnowShuDocker()
    target_adapter = PostgresAdapter(replica_metadata={})
    target_container, _ = shdocker.startup(
        target_adapter,
        'SnowflakeAdapter',
        [LOCAL_ARCHITECTURE.value],
        envars=['POSTGRES_USER=snowshu',
                'POSTGRES_PASSWORD=snowshu',
                'POSTGRES_DB=snowshu',
                f'PGDATA=/pgdata'])

    # load test data
    time.sleep(DOCKER_SPIN_UP_TIMEOUT)  # give pg a moment to spin up all the way
    # generate some test data
    engine = create_engine(
        f'postgresql://snowshu:snowshu@snowshu_target:9999/snowshu')
    engine.execute(
        f'CREATE TABLE {TEST_TABLE} (column_one VARCHAR, column_two INT)')
    engine.execute(
        f"INSERT INTO {TEST_TABLE} VALUES ('a',1), ('b',2), ('c',3)")

    checkpoint = engine.execute(f"SELECT * FROM {TEST_TABLE}").fetchall()

    assert ('a', 1) == checkpoint[0]

    # Dump replica data into shared volume
    target_container.exec_run(
        f"/bin/bash -c '{target_adapter.DOCKER_SHARE_REPLICA_DATA}'", tty=True)

    target_adapter.container = target_container
    target_container.stop()

    # check whether we can spin up a new replica from the shared volume
    # also, check where test data is available in the target db
    # repointing Postgres db to replica,  PGDATA
    target_container, _ = shdocker.startup(
        target_adapter,
        'SnowflakeAdapter',
        [LOCAL_ARCHITECTURE.value],
        envars=['POSTGRES_USER=snowshu',
                'POSTGRES_PASSWORD=snowshu',
                'POSTGRES_DB=snowshu',
                f'PGDATA=/pgdata'])

    # starting our new container
    target_container.start()
    # Load replica data from dump
    target_container.exec_run(
        f"/bin/bash -c '{target_adapter.DOCKER_IMPORT_REPLICA_DATA_FROM_SHARE}'", tty=True)

    engine = create_engine(
        'postgresql://snowshu:snowshu@snowshu_target:9999/snowshu')
    checkpoint = engine.execute(f"SELECT * FROM {TEST_TABLE}").fetchall()
    assert ('a', 1) == checkpoint[0]


def test_initialize_replica(docker_flush):
    with mock.patch('snowshu.adapters.target_adapters.postgres_adapter.PostgresAdapter._initialize_snowshu_meta_database', return_value=None):
        with mock.patch('snowshu.adapters.target_adapters.postgres_adapter.PostgresAdapter.target_database_is_ready', return_value=True):

            pg_adapter = PostgresAdapter(replica_metadata={})
            pg_adapter._credentials.host = 'snowshu_replica-integration-test'
            pg_adapter.target_arch = [LOCAL_ARCHITECTURE.value]

            pg_adapter.initialize_replica(source_adapter_name='SnowflakeAdapter')

            # check if container with name snowshu_replica-integration-test exists
            client = docker.from_env()
            assert client.containers.get(f'snowshu_replica-integration-test_{LOCAL_ARCHITECTURE.value}')

            # check if it has dependencies installed
            container = client.containers.get(f'snowshu_replica-integration-test_{LOCAL_ARCHITECTURE.value}')
            assert container.exec_run('psql --version').exit_code == 0
