import pytest
from snowshu.core.replica.replica_manager import ReplicaManager

@pytest.mark.skip
def tests_list_local(list_local_replicas):
    rep_list=ReplicaManager.list()

