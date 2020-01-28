import os
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.configs import PACKAGE_ROOT
from snowshu.core.models.relation import Relation


def test_gets_full_catalog(docker_flush):
    tp = ReplicaFactory()
    config = os.path.join(PACKAGE_ROOT, "snowshu", "templates", "replica.yml")
    tp.load_config(config)
    tp._load_full_catalog()

    for relation in tp.full_catalog:
        assert isinstance(relation, Relation)
