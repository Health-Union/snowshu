from pandas.core.frame import DataFrame

import pytest
import yaml

import snowshu.core.models.materializations as mz
from snowshu.core.models import data_types
from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.materializations import TABLE
from snowshu.adapters.target_adapters.postgres_adapter import PostgresAdapter
from snowshu.configs import DOCKER_REMOUNT_DIRECTORY
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.relation import Relation
from tests.assets.integration_test_setup import CREDENTIALS, get_connection_profile
from tests.common import rand_string


@pytest.fixture(scope='session')
def pg_adapter():
    adapter = PostgresAdapter(replica_metadata={})
    return adapter


def test_create_snowshu_schema_statement(pg_adapter):
    assert pg_adapter._create_snowshu_schema_statement() == 'CREATE SCHEMA IF NOT EXISTS snowshu;'


def test_quoted(pg_adapter):
    val = rand_string(10)
    assert val == pg_adapter.quoted(val)


def test_quoted_for_spaced_string(pg_adapter):
    val = rand_string(5) + ' ' + rand_string(6)
    assert f'"{val}"' == pg_adapter.quoted(val)


def test_is_fdw_schema(pg_adapter):
    schema = "DATASCIENCE_DEV"
    unique_databases = ["HU_DATA", "SNOWFLAKE_SAMPLE_DATA", "DEMO_DB", "UTIL_DB"]
    splitted = schema.split('__')
    assert pg_adapter.is_fdw_schema(schema, unique_databases) == (len(splitted) == 2 and splitted[0] in unique_databases)


def test_image_initialize_bash_commands(pg_adapter):
    PRELOADED_PACKAGES = ['postgresql-plpython3-12']
    commands = [f'apt-get update && apt-get install -y {" ".join(PRELOADED_PACKAGES)}']
    assert pg_adapter.image_initialize_bash_commands().sort() == commands.sort()


def test_build_snowshu_envars(pg_adapter):
    snowshu_envars = ['POSTGRES_USER=snowshu',
            'POSTGRES_PASSWORD=snowshu',
            'POSTGRES_DB=snowshu',
            f'PGDATA=/{DOCKER_REMOUNT_DIRECTORY}']
    envars = [f"{envar}=snowshu" for envar in snowshu_envars]
    envars.append(f"PGDATA=/{DOCKER_REMOUNT_DIRECTORY}")
    assert envars == pg_adapter._build_snowshu_envars(snowshu_envars)