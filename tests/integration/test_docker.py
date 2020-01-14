import pytest
import time
import docker
from tests.common import rand_string
from sqlalchemy import create_engine
from snowshu.core.replica.replica_manager import ReplicaManager
from snowshu.core.docker import SnowShuDocker
from snowshu.adapters.target_adapters import PostgresAdapter

from snowshu.logger import Logger
Logger().set_log_level(0)

@pytest.fixture
def kill_docker():
    yield
    shdocker=SnowShuDocker()
    shdocker.remove_container('snowshu_target')

def test_creates_replica():
    # build image
    # load it up with some data
    # convert it to a replica
    # spin it all down
    # start the replica
    # query it and confirm that the data is in there

    shdocker=SnowShuDocker()
    target_adapter=PostgresAdapter()
    target_container=shdocker.startup(
                        target_adapter.DOCKER_IMAGE,
                        target_adapter.DOCKER_START_COMMAND,
                        9999,
                        target_adapter.CLASSNAME,
                        
                        ['POSTGRES_USER=snowshu',
                         'POSTGRES_PASSWORD=snowshu',
                         'POSTGRES_DB=snowshu',])
    test_name, test_table =[rand_string(10) for _ in range(2)]


    ## load test data
    time.sleep(5) # give pg a moment to spin up all the way
    engine=create_engine('postgresql://snowshu:snowshu@snowshu_target:9999/snowshu')
    engine.execute(f'CREATE TABLE {test_table} (column_one VARCHAR, column_two INT)')
    engine.execute(f"INSERT INTO {test_table} VALUES ('a',1), ('b',2), ('c',3)")
    
    checkpoint=engine.execute(f"SELECT * FROM {test_table}").fetchall()
    assert ('a',1) == checkpoint[0] 

    replica=shdocker.convert_container_to_replica(test_name,
                                                  target_container, 
                                                  target_adapter)
    
    ## get a new replica
    test_replica=ReplicaManager().get_replica(test_name)
    time.sleep(5) # give pg a moment to spin up all the way
    engine=create_engine(f'postgresql://snowshu:snowshu@{test_replica.name}:9999/snowshu')
    res=engine.execute(f'SELECT * FROM {test_table}').fetchall() 
    assert ('a',1,) in res
    assert ('b',2,) in res
    assert ('c',3,) in res                                                                       
