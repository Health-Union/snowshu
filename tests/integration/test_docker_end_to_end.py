import time
from unittest import mock

import docker
from sqlalchemy import create_engine

from snowshu.adapters.target_adapters import PostgresAdapter
from snowshu.configs import DOCKER_REMOUNT_DIRECTORY, LOCAL_ARCHITECTURE
from snowshu.core.docker import SnowShuDocker
from snowshu.logger import Logger
from tests.common import rand_string
from tests.integration.snowflake.test_end_to_end import DOCKER_SPIN_UP_TIMEOUT

Logger().set_log_level(0, 0)

TEST_NAME, TEST_TABLE = [rand_string(10) for _ in range(2)]

def test_creates_replica(docker_flush):
    # build image
    # load it up with some data
    # convert it to a replica
    # spin it all down
    # start the replica
    # query it and confirm that the data is in there

    client = docker.from_env()

    arch_input_options = {
        'amd64': {
            'input_arch_list': ['amd64'],
            'result_images': ['latest', 'amd64'] if LOCAL_ARCHITECTURE == 'amd64' else ['amd64'],
            'active_container_arch': 'amd64',
            'passive_container_arch': None
        },
        'arm64': {
            'input_arch_list': ['arm64'],
            'result_images': ['latest', 'arm64'] if LOCAL_ARCHITECTURE == 'arm64' else ['arm64'],
            'active_container_arch': 'arm64',
            'passive_container_arch': None
        },
        'all': {
            # this case is different per machine type it runs in to save time
            'input_arch_list': ['arm64', 'amd64'] if LOCAL_ARCHITECTURE == 'arm64' else ['amd64', 'arm64'],
            'result_images': ['latest', 'arm64', 'amd64'],
            'active_container_arch': 'arm64' if LOCAL_ARCHITECTURE == 'arm64' else 'amd64',
            'passive_container_arch': 'amd64' if LOCAL_ARCHITECTURE == 'arm64' else 'arm64'
        }
    }

    for case_name, case_vars in arch_input_options.items():
        # docker_flush does not happen in between these loop cycles,
        # so containers of the same name get mixed up
        test_name_local = f'{TEST_NAME}-{case_name}'
        shdocker = SnowShuDocker()

        target_adapter = PostgresAdapter(replica_metadata={})
        target_container, passive_container = shdocker.startup(
            target_adapter,
            'SnowflakeAdapter',
            case_vars['input_arch_list'],
            envars=['POSTGRES_USER=snowshu',
                    'POSTGRES_PASSWORD=snowshu',
                    'POSTGRES_DB=snowshu',
                    f'PGDATA=/{DOCKER_REMOUNT_DIRECTORY}'])

        # add containers to adapter so that later target_adapter.copy_replica_data() works
        target_adapter.container = target_container
        target_adapter.passive_container = passive_container

        # assert if container architectures are as expected
        if passive_container:
            assert passive_container.name.split('_')[-1] == case_vars['passive_container_arch']
        assert target_container.name.split('_')[-1] == case_vars['active_container_arch']

        # load test data
        time.sleep(DOCKER_SPIN_UP_TIMEOUT)  # give pg a moment to spin up all the way
        engine = create_engine(
            'postgresql://snowshu:snowshu@snowshu_target:9999/snowshu')
        engine.execute(
            f'CREATE TABLE {TEST_TABLE} (column_one VARCHAR, column_two INT)')
        engine.execute(
            f"INSERT INTO {TEST_TABLE} VALUES ('a',1), ('b',2), ('c',3)")

        checkpoint = engine.execute(f"SELECT * FROM {TEST_TABLE}").fetchall()
        assert ('a', 1) == checkpoint[0]

        # copy data to passive container if exists
        target_adapter.copy_replica_data()

        replica_list = shdocker.convert_container_to_replica(test_name_local,
                                                             target_container,
                                                             passive_container)

        # assert correct replicas have been created
        # latest tag is attached to the same image instace as native arch one,
        # hence unnesting loop here
        arch_list_created_replicas = [tag.split(':')[1] for x in replica_list for tag in x.tags]
        assert sorted(arch_list_created_replicas) == sorted(case_vars['result_images'])

        for replica in replica_list:
            # get a new replica
            client = docker.from_env()

            client.containers.run(replica.id,
                                  ports={'9999/tcp': 9999},
                                  name=test_name_local,
                                  network='snowshu',
                                  detach=True)
            time.sleep(DOCKER_SPIN_UP_TIMEOUT)  # give pg a moment to spin up all the way
            engine = create_engine(
                f'postgresql://snowshu:snowshu@{test_name_local}:9999/snowshu')
            res = engine.execute(f'SELECT * FROM {TEST_TABLE}').fetchall()
            assert ('a', 1,) in res
            assert ('b', 2,) in res
            assert ('c', 3,) in res
            # verify that the extra OS packages are installed
            res = engine.execute("create extension plpython3u;")
            shdocker.remove_container(test_name_local)
