import pytest
import time
import docker
from tests.common import rand_string
from sqlalchemy import create_engine
from snowshu.core.docker import SnowShuDocker
from snowshu.adapters.target_adapters import PostgresAdapter
from tests.integration.snowflake.test_end_to_end import DOCKER_SPIN_UP_TIMEOUT

from snowshu.logger import Logger
Logger().set_log_level(0)

TEST_NAME, TEST_TABLE = [rand_string(10) for _ in range(2)]

def test_creates_replica(docker_flush):
    # build image
    # load it up with some data
    # convert it to a replica
    # spin it all down
    # start the replica
    # query it and confirm that the data is in there

    shdocker = SnowShuDocker()
    target_adapter = PostgresAdapter()
    target_container = shdocker.startup(
        target_adapter.DOCKER_IMAGE,
        target_adapter.DOCKER_START_COMMAND,
        9999,
        target_adapter.CLASSNAME,
        'SnowflakeAdapter',
        ['POSTGRES_USER=snowshu',
         'POSTGRES_PASSWORD=snowshu',
         'POSTGRES_DB=snowshu', ])

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

    replica = shdocker.convert_container_to_replica(TEST_NAME,
                                                    target_container,
                                                    target_adapter)
    # get a new replica
    client=docker.from_env()
    
    client.containers.run(replica.id,
                          ports={'9999/tcp':9999},
                          name=TEST_NAME,
                          network='snowshu',
                          detach=True)
    time.sleep(DOCKER_SPIN_UP_TIMEOUT)  # give pg a moment to spin up all the way
    engine = create_engine(
        f'postgresql://snowshu:snowshu@{TEST_NAME}:9999/snowshu')
    res = engine.execute(f'SELECT * FROM {TEST_TABLE}').fetchall()
    assert ('a', 1,) in res
    assert ('b', 2,) in res
    assert ('c', 3,) in res
    shdocker.remove_container(TEST_NAME)
