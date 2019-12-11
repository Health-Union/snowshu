import os
from snowshu.core.trail_path import TrailPath
from snowshu.utils import PACKAGE_ROOT
from snowshu.core.relation import Relation


def test_gets_full_catalog():
    tp = TrailPath()
    config=os.path.join(PACKAGE_ROOT,"tests","assets","integration","trail-path.yml")
    tp.load_config(config)    
    tp._load_full_catalog()
    
    for relation in tp.full_catalog:
        assert isinstance(relation, Relation)
