import json
import os
import re
import time
from datetime import datetime

import docker
import pytest
import sqlalchemy
import yaml
from click.testing import CliRunner
from sqlalchemy import create_engine

from snowshu.adapters.target_adapters.postgres_adapter import PostgresAdapter
from snowshu.core.docker import SnowShuDocker
from snowshu.configs import (DEFAULT_PRESERVE_CASE,
                             DEFAULT_MAX_NUMBER_OF_OUTLIERS,
                             DOCKER_REMOUNT_DIRECTORY,
                             DOCKER_TARGET_CONTAINER,
                             PACKAGE_ROOT)
from snowshu.core.main import cli
from snowshu.core.models import Relation, Attribute, data_types
from snowshu.core.models.materializations import TABLE

""" End-To-End all inclusive test session"""
# 1. builds a new replica based on the template configs
# 2. launches the replica
# 3. Queries the replica
# 4. Spins down and cleans up

BASE_CONN = 'postgresql://snowshu:snowshu@integration-test:9999/{}'
CONFIGURATION_PATH = os.path.join(PACKAGE_ROOT, 'tests', 'assets', 'replica_test_config.yml')
SNOWSHU_META_STRING = BASE_CONN.format('snowshu')
SNOWSHU_DEVELOPMENT_STRING = BASE_CONN.format('snowshu_development')
DOCKER_SPIN_UP_TIMEOUT = 15


@pytest.fixture(scope="session", autouse=True)
def end_to_end(docker_flush_session):
    runner = CliRunner()

    create_result = runner.invoke(cli, ('create', '--replica-file', CONFIGURATION_PATH, '--barf'))
    if create_result.exit_code:
        print(create_result.exc_info)
        raise create_result.exception
    create_output = create_result.output.split('\n')
    client = docker.from_env()
    client.containers.run('snowshu_replica_integration-test',
                          ports={'9999/tcp': 9999},
                          name='integration-test',
                          network='snowshu',
                          detach=True)
    time.sleep(DOCKER_SPIN_UP_TIMEOUT)  # the replica needs a second to initialize
    return create_output


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
    assert any_appearance_of('Done replication of relation snowshu_development.source_system.order_items', result_lines)


@pytest.mark.skip
def test_snowshu_explain(end_to_end):
    runner = CliRunner()
    response = json.loads(runner.invoke(
        cli, ('explain', 'integration-test', '--json')))

    assert response['name'] == 'integration-test'
    assert response['image'] == 'postgres:12'
    assert response['target_adapter'] == 'postgres'
    assert response['source_adapter'] == 'snowflake'
    assert datetime(response['created_at']) < datetime.now()


def test_incremental_build_with_override_image(end_to_end):
    runner = CliRunner()
    create_result = runner.invoke(cli, ('create'))
    if create_result.exit_code:
        print(create_result.exc_info)
        raise create_result.exception
    create_output = create_result.output.split('\n')
    assert any_appearance_of('Container initialized.', create_output)

    create_result = runner.invoke(cli, ('create', '--incremental', 'snowshu_integration-test:latest'))
    if create_result.exit_code:
        print(create_result.exc_info)
        raise create_result.exception
    create_output = create_result.output.split('\n')
    assert any_appearance_of('Container initialized.', create_output)