import os
import pytest
from snowshu.core.replica import Replica
from snowshu.utils import PACKAGE_ROOT

def test_analyze_unsampled():

    replica=Replica()

    config=os.path.join(PACKAGE_ROOT,"snowshu","templates","replica.yml")
    replica.load_config(config) 
    result=replica.analyze()

    for line in result.split('\n'):
        if "ORDERS" in line:
            assert "\x1b[0;32m100\x1b[0m" in line   
