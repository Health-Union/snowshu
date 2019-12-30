import os
import pytest
from snowshu.core.replica import Replica
from snowshu.utils import PACKAGE_ROOT

def test_analyze_unsampled():

    replica=Replica()

    config=os.path.join(PACKAGE_ROOT,"tests","assets","integration","replica.yml")
    replica.load_config(config) 
    result=replica.analyze()

    for line in result.split('\n'):
        if "ORDERS" in line:
            assert "100 %" in line   
