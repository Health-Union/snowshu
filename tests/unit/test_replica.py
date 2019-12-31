from snowshu.core.replica import Replica
from tests.common import rand_string
import tempfile
import pytest
import mock
import networkx
import yaml
import json
from io import StringIO
from snowshu.adapters.source_adapters.sample_methods import BernoulliSample 
import snowshu.core.models.data_types as dt
from snowshu.core.models.materializations import TABLE,VIEW
from snowshu.core.models.relation import Relation
from snowshu.core.models.attribute import Attribute
from snowshu.adapters.source_adapters import SnowflakeAdapter

def test_errors_on_bad_profile(stub_configs):
    replica=Replica()
    stub_configs=stub_configs()
    SOURCE_PROFILE,TARGET_PROFILE,STORAGE_PROFILE=[rand_string(10) for _ in range(3)]
    stub_configs['source']['profile']=SOURCE_PROFILE
    stub_configs['target']['profile']=TARGET_PROFILE
    stub_configs['storage']['profile']=STORAGE_PROFILE
    
    with pytest.raises(AttributeError):
        mock_config_file=StringIO(yaml.dump(stub_configs))   
        replica.load_config(mock_config_file)

def test_loads_good_creds(stub_creds):
    replica=Replica()
    stub_creds=stub_creds()
    SOURCES_NAME,SOURCES_PASSWORD,STORAGES_ACCOUNT=[rand_string(10) for _ in range(3)]
    with tempfile.NamedTemporaryFile(mode='w') as mock_file:
        stub_creds['sources'][0]['name']=SOURCES_NAME
        stub_creds['sources'][0]['password']=SOURCES_PASSWORD
        stub_creds['storages'][0]['account']=STORAGES_ACCOUNT
        json.dump(stub_creds,mock_file)
        mock_file.seek(0)
        replica._load_credentials(mock_file.name,
                                 SOURCES_NAME,
                                 'default')

    assert replica._credentials['source']['name'] == SOURCES_NAME
    assert replica._credentials['source']['password'] == SOURCES_PASSWORD
    assert replica._credentials['storage']['account'] == STORAGES_ACCOUNT
        
def test_sets_good_source_adapter(stub_configs):
    config=StringIO(yaml.dump(stub_configs()))
    replica=Replica()
    replica.load_config(config)

    assert isinstance(replica.source_adapter,SnowflakeAdapter)

def test_rejects_bad_adapter():
    replica=Replica()
    with pytest.raises(KeyError):
        replica._fetch_adapter('european_plug_adapter','source')

