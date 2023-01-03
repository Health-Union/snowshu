import os
import time
from unittest.mock import patch

import docker
import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

from snowshu.configs import PACKAGE_ROOT
from snowshu.core.main import cli


DOCKER_SPIN_UP_TIMEOUT = 10
# This one is actually a base
INCREMENTAL_CONFIGURATION_PATH = os.path.join(
    PACKAGE_ROOT, 'tests', 'assets', 'replica_test_incremental_config.yml')
# This one has added relations compared to the one above
CONFIGURATION_PATH = os.path.join(
    PACKAGE_ROOT, 'tests', 'assets', 'replica_test_config.yml')

EXPECTED_BASE_TABLE_LIST = [
    'address_region_attributes',
    'case_testing',
    'data_types',
    'order_items',
    'order_items_view',
    'orders',
    'products',
    'social_users_import',
    'user_addresses',
    'user_cookies',
    'users']

EXPECTED_INREMENTED_TABLE_LIST = [
    'address_region_attributes',
    'case_testing',
    'child_type_0_items',
    'child_type_1_items',
    'child_type_2_items',
    'data_types',
    'order_items',
    'order_items_view',
    'orders',
    'parent_table',
    'parent_table_2',
    'products',
    'social_users_import',
    'user_addresses',
    'user_cookies',
    'users'
]


@pytest.fixture(scope="module")
def base_for_incremental(docker_flush_module):
    runner = CliRunner()
    runner.invoke(cli, ('create', '--replica-file',
                  INCREMENTAL_CONFIGURATION_PATH, '--multiarch'))


def get_tables(hostname: str) -> list:
    """Helper function that gets a list of tables from replica"""

    conn_string = f'postgresql://snowshu:snowshu@{hostname}:9999/snowshu'
    query_string = '''
        select T.TABLE_NAME
        from SNOWSHU.INFORMATION_SCHEMA.TABLES T
        where T.TABLE_SCHEMA like 'snowshu_development%%'
        order by 1
        '''

    conn = create_engine(conn_string)
    query = conn.execute(query_string)
    results = query.fetchall()
    results = [row[0] for row in results]
    return results


def test_base_replica(base_for_incremental):
    """Tests if base provided by 'base_for_inremental' is correct"""

    client = docker.from_env()

    for fake_local_arch in ['amd64', 'arm64']:
        with patch('snowshu.core.main.LOCAL_ARCHITECTURE', fake_local_arch) as _:
            # Run base replica and verify it has correct set of tables
            container = client.containers.run(f'snowshu_replica_integration-test-incremental:{fake_local_arch}',
                                              ports={'9999/tcp': 9999},
                                              name='integration-test-incremental',
                                              network='snowshu',
                                              detach=True)

            time.sleep(DOCKER_SPIN_UP_TIMEOUT)

            assert get_tables(
                'integration-test-incremental') == EXPECTED_BASE_TABLE_LIST
            container.remove(force=True)


def test_single_arch_incremental(base_for_incremental):
    """Runs a 'create -i' command and tests the result
       WARNING: replica tagged as snowshu_replica_integration-test-incremental is actually a base,
                and replica tagged as snowshu_replica_integration-test is the result of incremental build
    """

    runner = CliRunner()
    client = docker.from_env()

    for fake_local_arch in ['amd64', 'arm64']:
        with patch('snowshu.core.main.LOCAL_ARCHITECTURE', fake_local_arch) as _:
            runner.invoke(cli, ('create',
                                '-i',
                                f'snowshu_replica_integration-test-incremental:{fake_local_arch}',
                                '--replica-file',
                                CONFIGURATION_PATH),
                          catch_exceptions=False)

            # Check if all images are present
            new_replicas_list = client.images.list(
                'snowshu_replica_integration-test')
            assert len(new_replicas_list) == 1

            # Check if data is fine
            container = client.containers.run(f'snowshu_replica_integration-test:{fake_local_arch}',
                                              ports={'9999/tcp': 9999},
                                              name='integration-test',
                                              network='snowshu',
                                              detach=True)
            time.sleep(DOCKER_SPIN_UP_TIMEOUT)

            assert get_tables(
                'integration-test') == EXPECTED_INREMENTED_TABLE_LIST
            container.remove(force=True)

            # Remove new images before building another incremental
            for image in client.images.list('snowshu_replica_integration-test'):
                for tag in image.tags:
                    client.images.remove(tag)


def test_multi_arch_incremental(base_for_incremental):
    """Runs a 'create -i xxx -m' command and tests the result
       WARNING: replica tagged as snowshu_replica_integration-test-incremental is actually a base,
                and replica tagged as snowshu_replica_integration-test is the result of incremental build
    """

    runner = CliRunner()
    client = docker.from_env()

    for fake_local_arch in ['arm64', 'arm64']:
        for image_arch in ['amd64', 'arm64']:
            with patch('snowshu.core.main.LOCAL_ARCHITECTURE', fake_local_arch) as _, \
                 patch('snowshu.core.docker.LOCAL_ARCHITECTURE', fake_local_arch) as _:

                result = runner.invoke(cli, ('create',
                                            '-i',
                                            f'snowshu_replica_integration-test-incremental:{image_arch}',
                                            '--replica-file',
                                            CONFIGURATION_PATH,
                                            '-m'),
                                    catch_exceptions=False)
                output = result.output
                # Check if warning is printed
                if fake_local_arch != image_arch:
                    assert 'Supplied base image is of a non-native architecture, please try to use native for better performance' in output
                else:
                    assert 'Supplied base image is of a non-native architecture, please try to use native for better performance' not in output

                # Check if existing records are picked up
                assert f'Found a total of {len(EXPECTED_BASE_TABLE_LIST)} relations from the database' in output

                new_replicas_list = client.images.list(
                    'snowshu_replica_integration-test')
                assert len(new_replicas_list) == 2

                # Check if all new replicas have correct set of tables
                for replica in new_replicas_list:
                    container = client.containers.run(replica,
                                                    ports={'9999/tcp': 9999},
                                                    name='integration-test',
                                                    network='snowshu',
                                                    detach=True)
                    time.sleep(DOCKER_SPIN_UP_TIMEOUT)

                    assert get_tables(
                        'integration-test') == EXPECTED_INREMENTED_TABLE_LIST
                    container.remove(force=True)

                for image in client.images.list('snowshu_replica_integration-test'):
                    for tag in image.tags:
                        client.images.remove(tag)
