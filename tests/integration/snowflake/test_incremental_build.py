import os
import time
from unittest.mock import patch

import docker
import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

from snowshu.configs import PACKAGE_ROOT
from snowshu.core.main import cli


DOCKER_SPIN_UP_TIMEOUT = 4
# This one is actually a base
INCREMENTAL_CONFIGURATION_PATH = os.path.join(
    PACKAGE_ROOT, 'tests', 'assets', 'replica_test_incremental_config.yml')
# This one has added relations compared to the one above
CONFIGURATION_PATH = os.path.join(
    PACKAGE_ROOT, 'tests', 'assets', 'replica_test_config.yml')


@pytest.fixture(scope="module")
def base_for_incremental(docker_flush_module):
    runner = CliRunner()
    runner.invoke(cli, ('create', '--replica-file',
                  INCREMENTAL_CONFIGURATION_PATH, '--multiarch'))


def test_incremental_build(base_for_incremental):
    ''' Creates incremental builds of normal and multiarch variants, checks if they contain specified tables
        WARNING: As of now, replica (config file, hostname, image and container name) for BASE is called INCREMENTAL, and visa versa.
    '''
    runner = CliRunner()
    client = docker.from_env()

    expected_base_table_list = [
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

    expected_incremented_table_list = [
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

    def get_tables(hostname: str) -> list:
        conn_string = f'postgresql://snowshu:snowshu@{hostname}:9999/snowshu'
        query_string = '''select T.TABLE_NAME from SNOWSHU.INFORMATION_SCHEMA.TABLES T where T.TABLE_SCHEMA like 'snowshu_development%%' order by 1'''

        conn = create_engine(conn_string)
        query = conn.execute(query_string)
        results = query.fetchall()
        results = [row[0] for row in results]
        return results

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
                'integration-test-incremental') == expected_base_table_list
            container.remove(force=True)

            # Do a native (of temporarily "native" arch) build
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
                'integration-test') == expected_incremented_table_list
            container.remove(force=True)
            
            # Remove new images before building another incremental
            for image in client.images.list('snowshu_replica_integration-test'):
                for tag in image.tags:
                    client.images.remove(tag)

            # Do a multiarch incremental build
            runner.invoke(cli, ('create',
                                '-i',
                                f'snowshu_replica_integration-test-incremental:{fake_local_arch}',
                                '--replica-file',
                                CONFIGURATION_PATH,
                                '-m'),
                                catch_exceptions=False)

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
                    'integration-test') == expected_incremented_table_list
                container.remove(force=True)

            for image in client.images.list('snowshu_replica_integration-test'):
                for tag in image.tags:
                    client.images.remove(tag)
