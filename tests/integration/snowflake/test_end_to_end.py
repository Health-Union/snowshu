import json
import os
import re
from datetime import datetime

import pytest
import sqlalchemy
import yaml
import docker
from click.testing import CliRunner
from sqlalchemy import create_engine

from snowshu.adapters.target_adapters.postgres_adapter import PostgresAdapter
from snowshu.core.docker import SnowShuDocker
from snowshu.configs import (DEFAULT_PRESERVE_CASE,
                             DEFAULT_MAX_NUMBER_OF_OUTLIERS,
                             DOCKER_REMOUNT_DIRECTORY,
                             DOCKER_TARGET_CONTAINER,
                             DOCKER_REPLICA_VOLUME,
                             DOCKER_NETWORK,
                             PACKAGE_ROOT,
                             LOCAL_ARCHITECTURE,
                             POSTGRES_IMAGE
                             )
from snowshu.core.main import cli
from snowshu.core.models import Relation, Attribute, data_types
from snowshu.core.models.materializations import TABLE

""" End-To-End all inclusive test session"""
# 1. builds a new replica based on the template configs
# 2. launches the replica
# 3. Queries the replica
# 4. Spins down and cleans up

BASE_CONN = 'postgresql://snowshu:snowshu@integration-test:9999/{}'
INITIAL_INCREMENTAL_CONFIG_PATH = os.path.join(PACKAGE_ROOT,
                                               'tests',
                                               'assets',
                                               'replica_test_incremental_config.yml')
CONFIGURATION_PATH = os.path.join(PACKAGE_ROOT, 'tests', 'assets', 'replica_test_config.yml')
SNOWSHU_META_STRING = BASE_CONN.format('snowshu')
SNOWSHU_DEVELOPMENT_STRING = BASE_CONN.format('snowshu_development')
DOCKER_SPIN_UP_TIMEOUT = 15


def any_appearance_of(string, strings):
    return any([string in line for line in strings])


def find_number_of_processed_relations(strings):
    regex = r"Identified a total of (.*?) relations to sample based on the specified configurations."
    for string in strings:
        substring = re.search(regex, string)
        if substring:
            found_relations_count = int(substring.group(1))
            break
    return found_relations_count if substring else 0


def test_reports_full_catalog_start(end_to_end):
    result_lines = end_to_end
    assert any_appearance_of('Building filtered catalog...', result_lines)


def test_finds_n_relations(end_to_end):
    result_lines = end_to_end
    assert find_number_of_processed_relations(result_lines) == 16, \
        "Number of found relations do not match the expected of 16 relations. Check database."


def test_replicates_order_items(end_to_end):
    result_lines = end_to_end
    assert any_appearance_of('Done replication of relation SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS', result_lines)


@pytest.mark.skip
def test_snowshu_explain(end_to_end):
    runner = CliRunner()
    response = json.loads(runner.invoke(
        cli, ('explain', 'integration-test', '--json')))

    assert response['name'] == 'integration-test'
    assert response['image'] == POSTGRES_IMAGE
    assert response['target_adapter'] == 'postgres'
    assert response['source_adapter'] == 'snowflake'
    assert datetime(response['created_at']) < datetime.now()


def test_replica_meta(end_to_end):
    conn = create_engine(SNOWSHU_META_STRING)
    query_string = """
        SELECT * FROM SNOWSHU.SNOWSHU.REPLICA_META
    """
    with open(CONFIGURATION_PATH, 'r') as file_stream:
        expected_config = yaml.safe_load(file_stream)
    # default for preserve case and max_outliers are set after parsing
    expected_config["preserve_case"] = DEFAULT_PRESERVE_CASE
    expected_config["source"]["max_number_of_outliers"] = DEFAULT_MAX_NUMBER_OF_OUTLIERS

    query = conn.execute(query_string)
    results = query.fetchall()
    for record in results:
        assert record['created_at']
        # values from replica_test_config.yml
        assert record['name'] == 'integration-test'
        assert record['short_description'] == 'this is a sample with LIVE CREDS for integration'
        assert record['long_description'] == 'this is for testing against a live db'
        assert record['config_json'] == expected_config

    assert len(results) == 1


def test_polymorphic_parent_id(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query_string = """
        SELECT * FROM SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.PARENT_TABLE
    """
    query = conn.execute(query_string)
    results = query.fetchall()
    for record in results:
        assert record['id'] not in (13, 14)
    assert len(results) == 12


def test_polymorphic_child_id(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query_string = """
        SELECT * FROM SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.PARENT_TABLE_2
    """
    query = conn.execute(query_string)
    results = query.fetchall()
    for record in results:
        assert record['id'] not in (13, 14)
    assert len(results) == 12


def test_polymorphic_child_tables(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    for val in ['0', '1', '2']:
        query_string = f"""
            SELECT * FROM SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.CHILD_TYPE_{val}_ITEMS
        """
        query = conn.execute(query_string)
        results = query.fetchall()
        assert len(results) == 4


def test_bidirectional(end_to_end):
    print('test_bidirectional')
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
        SELECT 
            COUNT(*) 
        FROM 
            SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.USER_COOKIES uc
        FULL OUTER JOIN
             SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.USERS u
        ON 
            uc.user_id=u.id
        WHERE 
            uc.user_id IS NULL
        OR
            u.id IS NULL
        """
    q = conn.execute(query)
    count = q.fetchall()[0][0]
    assert count == 0


def test_directional(end_to_end):
    print('test_directional')
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
        WITH
        joined_roots AS (
        SELECT 
            oi.id AS oi_id
            ,oi.order_id AS oi_order_id
            ,o.id AS o_id
        FROM 
            SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS oi
        FULL OUTER JOIN
             SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDERS o
        ON 
            oi.order_id = o.id
        )
        SELECT 
            (SELECT COUNT(*) FROM joined_roots WHERE oi_id is null) AS upstream_missing
            ,(SELECT COUNT(*) FROM joined_roots WHERE o_id is null) AS downstream_missing
        """
    q = conn.execute(query)
    upstream_missing, downstream_missing = q.fetchall()[0]
    assert upstream_missing > 0
    # it is statistically very unlikely that NONE of the upstreams without a downstream will be included.
    assert downstream_missing == 0


def test_view(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
        SELECT 
            (SELECT COUNT(*) FROM SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS_VIEW) /
            (SELECT COUNT(*) FROM SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS) AS delta
        """
    q = conn.execute(query)
    assert len(set(q.fetchall()[0])) == 1


def test_cross_database_query(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = 'SELECT COUNT(*) FROM snowshu__snowshu.replica_meta'
    q = conn.execute(query)
    assert len(set(q.fetchall()[0])) == 1


def test_applies_emulation_function(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = 'SELECT ANY_VALUE(id) FROM SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS'
    q = conn.execute(query)
    assert int(q.fetchall()[0][0]) > 0


def test_applies_uuid_emulation_function(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = 'SELECT UUID_STRING()'
    q = conn.execute(query)
    assert re.match('[0-9A-Fa-f-]{36}', q.fetchall()[0][0])


def test_applies_pg_extensions(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = "SELECT CASE WHEN 'My_Cased_String'::citext = 'my_cased_string'::citext THEN 'SUCCESS' ELSE 'FAIL' END"
    q = conn.execute(query)
    assert q.fetchall()[0][0] == 'SUCCESS'


def test_data_types(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE
        FROM 
            SNOWSHU_DEVELOPMENT.information_schema.columns 
        WHERE 
            TABLE_SCHEMA = 'tests_data' 
        AND 
            TABLE_NAME='data_types'
        """

    q = conn.execute(query)
    type_mappings = q.fetchall()
    EXPECTED_DATA_TYPES = {
        "array_col": "json",
        "bigint_col": "bigint",
        "binary_col": "bytea",
        "boolean_col": "boolean",
        "char_col": "character varying",
        "character_col": "character varying",
        "date_col": "date",
        "datetime_col": "timestamp without time zone",
        "decimal_col": "bigint",
        "double_col": "double precision",
        "doubleprecision_col": "double precision",
        "float_col": "double precision",
        "float4_col": "double precision",
        "float8_col": "double precision",
        "int_col": "bigint",
        "integer_col": "bigint",
        "number_col": "bigint",
        "numeric_col": "bigint",
        "object_col": "json",
        "real_col": "double precision",
        "smallint_col": "bigint",
        "string_col": "character varying",
        "text_col": "character varying",
        "time_col": "time without time zone",
        "timestamp_col": "timestamp without time zone",
        "timestamp_ntz_col": "timestamp without time zone",
        "timestamp_ltz_col": "timestamp with time zone",
        "timestamp_tz_col": "timestamp with time zone",
        "varbinary_col": "bytea",
        "varchar_col": "character varying",
        "variant_col": "json"
    }
    assert {t[0]: t[1] for t in type_mappings} == EXPECTED_DATA_TYPES


def test_casing(end_to_end):
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE
        FROM 
            SNOWSHU_DEVELOPMENT.information_schema.columns 
        WHERE 
            TABLE_SCHEMA = 'tests_data' 
        AND 
            TABLE_NAME='case_testing'
        """

    q = conn.execute(query)
    type_mappings = q.fetchall()
    EXPECTED_DATA_TYPES = {
        "lower_col": "character varying",
        "upper_col": "character varying",  # fully upper-case should map to all lower (snowflake -> postgres)
        "CamelCasedCol": "character varying",
        "quoted_upper_col": "character varying",  # fully upper-case should map to all lower (snowflake -> postgres)
        "1": "character varying",
        "Spaces Col": "character varying",
        "UNIFORM SPACE": "character varying",
        "uniform lower": "character varying",
        "Snake_Case_Camel_Col": "character varying",
    }
    assert {t[0]: t[1] for t in type_mappings} == EXPECTED_DATA_TYPES


def test_get_relations_from_database(end_to_end):
    adapter = PostgresAdapter(replica_metadata={})
    if adapter.target != "localhost":
        adapter._credentials.host = 'integration-test'

    config_patterns = [
        dict(database="snowshu",
             schema=".*",
             name=".*")
    ]

    attributes = [
        Attribute('created_at', data_types.TIMESTAMP_TZ),
        Attribute('config_json', data_types.JSON),
        Attribute('name', data_types.VARCHAR),
        Attribute('short_description', data_types.VARCHAR),
        Attribute('long_description', data_types.VARCHAR)
    ]
    relation = Relation("snowshu", "snowshu", "replica_meta", TABLE, attributes)

    catalog = adapter.build_catalog(config_patterns, thread_workers=1)
    relations = []
    for rel in catalog:
        relations.append(rel.__dict__.items())
    assert relation.__dict__.items() in relations


def test_x_db_incremental_import(end_to_end):
    adapter = PostgresAdapter(replica_metadata={})
    if adapter.target != "localhost":
        adapter._credentials.host = 'integration-test'

    def successfully_enabled_without_errors(adapter):
        try:
            adapter.enable_cross_database()
            adapter.enable_cross_database()
            unique_databases = set(adapter._get_all_databases())
            unique_databases.remove('postgres')
            schemas_len = []
            for database in unique_databases:
                for schema in adapter._get_all_schemas(database, True):
                    schemas_len.append(len(schema.split('__')))
            assert all(x <= 2 for x in schemas_len)
            return True
        except sqlalchemy.exc.ProgrammingError:
            return False

    assert successfully_enabled_without_errors(adapter)


def test_using_different_image(end_to_end):
    client = docker.from_env()
    shdocker = SnowShuDocker()
    target_adapter = PostgresAdapter(replica_metadata={})

    replica_volume = shdocker._create_snowshu_volume(DOCKER_REPLICA_VOLUME)
    network = shdocker._get_or_create_network(DOCKER_NETWORK)

    target_adapter.DOCKER_TARGET_PORT = 9990
    envars = ['POSTGRES_USER=snowshu',
              'POSTGRES_PASSWORD=snowshu',
              'POSTGRES_DB=snowshu',
              f'PGDATA=/{DOCKER_REMOUNT_DIRECTORY}']

    target_container = shdocker.create_and_init_container(
        image=client.images.get('snowshu_replica_integration-test'),
        target_adapter=target_adapter,
        source_adapter='SnowflakeAdapter',
        container_name=DOCKER_TARGET_CONTAINER,
        network=network,
        replica_volume=replica_volume,
        envars=envars
        )
    assert target_container.status == 'created'
    assert target_container.image.tags[0] in [f'snowshu_replica_integration-test:{LOCAL_ARCHITECTURE}',
                                               'snowshu_replica_integration-test:latest']
    target_container.start()
    target_container.reload()
    assert target_container.status == 'running'
    target_container.remove(force=True)
