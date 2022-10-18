import os

from snowshu.configs import LOCAL_ARCHITECTURE, PACKAGE_ROOT
from snowshu.core.replica.replica_factory import ReplicaFactory


def test_analyze_unsampled(docker_flush):

    replica = ReplicaFactory()

    config = os.path.join(PACKAGE_ROOT, "tests", "assets", "replica_test_config.yml")
    replica.load_config(config, target_arch=[LOCAL_ARCHITECTURE])
    result = replica.analyze(barf=False, retry_count=1).split('\n')
    result.reverse()
    for line in result:
        if "ORDERS" in line:
            assert '\x1b[0;32m100 %\x1b[0m' in line
            break
