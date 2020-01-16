import os
import pytest
import docker
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.utils import PACKAGE_ROOT
from snowshu.core.docker import SnowShuDocker

@pytest.fixture(autouse=True)
def spin_down_dockers():
    shdocker=SnowShuDocker()
    shdocker.remove_container('snowshu_target')
    shdocker.remove_container('integration-test')
    yield

def test_analyze_unsampled():

    replica=ReplicaFactory()

    config=os.path.join(PACKAGE_ROOT,"snowshu","templates","replica.yml")
    replica.load_config(config) 
    result=replica.analyze().split('\n')
    result.reverse()
    for line in result:
        if "ORDERS" in line:
            assert '\x1b[0;32m100\x1b[0m' in line
            break  
