import os
import pytest
import docker
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.utils import PACKAGE_ROOT
from snowshu.core.docker import SnowShuDocker

def test_analyze_unsampled(docker_flush):

    replica = ReplicaFactory()

    config = os.path.join(PACKAGE_ROOT, "snowshu", "templates", "replica.yml")
    replica.load_config(config)
    result = replica.analyze(barf=False).split('\n')
    result.reverse()
    for line in result:
        if "ORDERS" in line:
            assert '\x1b[0;32m100 %\x1b[0m' in line
            break
