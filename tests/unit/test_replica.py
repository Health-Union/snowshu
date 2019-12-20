import networkx
import sqlalchemy
from snowshu.core.models.materializations import TABLE,VIEW
from snowshu.core.models.attribute import Attribute
from snowshu.source_adapters.sample_methods import BernoulliSample 
import snowshu.core.models.data_types as dt
from snowshu.core.models.relation import Relation
import mock
import os
from snowshu.utils import PACKAGE_ROOT
from tests.common import rand_string
import pytest
import yaml
from io import StringIO
from snowshu.source_adapters import SnowflakeAdapter
from snowshu.core.replica import Replica
from dataclasses import dataclass
from snowshu.logger import Logger

log_engine=Logger()
#log_engine.set_log_level(30)
logger=log_engine.logger



@pytest.fixture
def randomized_config():
    with open(os.path.join(PACKAGE_ROOT,'tests','assets','unit','replica-v1.yml'),'r') as config:
        test_replica=yaml.safe_load(config)
    
    @dataclass
    class Expected:
        DRELATION:str
        BIRELATION:str
        DRELATION_SCHEMA:str 
        BIRELATION_SCHEMA:str
        BIRELATION_LOCAL_ATTRIBUTE:str
        BIRELATION_REMOTE_ATTRIBUTE:str
        DRELATION_LOCAL_ATTRIBUTE:str
        DRELATION_REMOTE_ATTRIBUTE:str

        ## creds
        SOURCES_NAME:str
        SOURCES_ADAPTER:str
        SOURCES_ACCOUNT:str
        SOURCES_PASSWORD:str
        SOURCES_USERNAME:str
        SOURCES_DATABASE:str
        
        TARGETS_NAME:str
        TARGETS_ADAPTER:str
        TARGETS_ACCOUNT:str
        TARGETS_PASSWORD:str
        TARGETS_USERNAME:str


        STORAGES_NAME:str
        STORAGES_ADAPTER:str
        STORAGES_ACCOUNT:str
        STORAGES_PASSWORD:str
        STORAGES_USERNAME:str
        CREDPATH:str=f"{PACKAGE_ROOT}/tests/assets/unit/credentials.yml"
    
    expected=Expected(*[rand_string(10) for _ in range(24)])

   
    # randomize values
    relationships=test_replica['source']['relations'][0]['relationships']
    directional=relationships['directional'][0]
    directional['local_attribute']=expected.DRELATION_LOCAL_ATTRIBUTE
    directional['remote_attribute']=expected.DRELATION_REMOTE_ATTRIBUTE
    directional['relation']=expected.DRELATION
    directional['schema']=expected.DRELATION_SCHEMA
    bidirectional=relationships['bidirectional'][0]
    bidirectional['local_attribute']=expected.BIRELATION_LOCAL_ATTRIBUTE
    bidirectional['remote_attribute']=expected.BIRELATION_REMOTE_ATTRIBUTE
    bidirectional['relation']=expected.BIRELATION
    bidirectional['schema']=expected.BIRELATION_SCHEMA
    relationships['directional'][0]=directional
    relationships['bidirectional'][0]=bidirectional
    test_replica['source']['relations'][0]['relationships']=relationships
    test_replica['credpath']=expected.CREDPATH

    as_dict=test_replica
    as_file_object=StringIO(yaml.dump(as_dict))

    random_creds_as_dict = dict( version="2",
          sources=[dict(
                        name=expected.SOURCES_NAME,
                        adapter=expected.SOURCES_ADAPTER,
                        account=expected.SOURCES_ACCOUNT,
                        password=expected.SOURCES_PASSWORD,
                        user=expected.SOURCES_USERNAME,
                        database=expected.SOURCES_DATABASE)],
          targets=[dict(
                        name=expected.TARGETS_NAME,
                        adapter=expected.TARGETS_ADAPTER,
                        account=expected.TARGETS_ACCOUNT,
                        password=expected.TARGETS_PASSWORD,
                        username=expected.TARGETS_USERNAME)],
          storages=[dict(

                        name=expected.STORAGES_NAME,
                        adapter=expected.STORAGES_ADAPTER,
                        account=expected.STORAGES_ACCOUNT,
                        password=expected.STORAGES_PASSWORD,
                        username=expected.STORAGES_USERNAME)])
    random_creds_as_file_object=StringIO(yaml.dump(random_creds_as_dict))

    yield as_file_object, as_dict, expected, random_creds_as_file_object, random_creds_as_dict,


   
def test_errors_on_bad_profile(randomized_config):
    _,config_dict,expected,__,___=randomized_config
    tp=Replica()
    SOURCE_PROFILE,TARGET_PROFILE,STORAGE_PROFILE=[rand_string(10) for _ in range(3)]
    config_dict['source']['profile']=SOURCE_PROFILE
    config_dict['target']['profile']=TARGET_PROFILE
    config_dict['storage']['profile']=STORAGE_PROFILE
    with pytest.raises(AttributeError):
        tp.load_config(StringIO(yaml.dump(config_dict)))


def test_loads_good_creds(randomized_config):
    config,_,expected,creds,__ =randomized_config
    replica=Replica()
    
    replica._load_credentials(creds,
                                 expected.SOURCES_NAME,
                                 expected.TARGETS_NAME,
                                 expected.STORAGES_NAME)
    assert replica._credentials['source']['password'] == expected.SOURCES_PASSWORD
    assert replica._credentials['target']['username'] == expected.TARGETS_USERNAME
    assert replica._credentials['storage']['account'] == expected.STORAGES_ACCOUNT
        
def test_sets_good_source_adapter(randomized_config):
    config,_,expected,__,___ = randomized_config
    replica=Replica()
    replica.load_config(config)

    assert isinstance(replica.source_adapter,SnowflakeAdapter)

def test_rejects_bad_adapter():
    replica=Replica()
    with pytest.raises(KeyError):
        replica._fetch_adapter('european_plug_adapter','source')

def test_builds_dags_regex():
    ## this test is deprecated to the utils suite
    tp=Replica()
    NEVER_RELATION=Relation(database='RAWDATABASE',
                 schema='NOT_MY_SCHEMA',
                 name='NOT_MY_TABLE',
                 materialization=VIEW, 
                 attributes=[Attribute('hot_dog',dt.INTEGER),Attribute('shoes',dt.DATE)])
    DEFAULT_MATCHED_RELATION=Relation(database='RAWDATABASE',
                 schema='RAWSCHEMA',
                 name='RAWVIEW',
                 materialization=VIEW, 
                 attributes=[Attribute('hot_dog',dt.INTEGER),Attribute('shoes',dt.DATE)])
    DEPENDENT_RELATION=Relation(database='PRODDATABASE',
                 schema='SNOWSCHEMA',
                 name='COLDTABLE',
                 materialization=TABLE,
                 attributes=[Attribute('banana',dt.VARCHAR)])
    SPECIFIED_RELATION=Relation(database='RAWDATABASE',
                 materialization=TABLE,
                 schema='SNOWSCHEMA',
                 name='FROSTY',
                 attributes=[Attribute('hot_dog_id',dt.INTEGER),Attribute('not_hotdog',dt.VARCHAR)])           
    ISO_RELATION=Relation(database='RAWDATABASE',
                          schema='RAWSCHEMA',
                          materialization=VIEW,
                          name="ISO_VIEW",
                          attributes=[Attribute('thing',dt.INTEGER)])
    TEST_RELATIONS=(NEVER_RELATION,DEFAULT_MATCHED_RELATION,DEPENDENT_RELATION,SPECIFIED_RELATION,ISO_RELATION,)

    TEST_BIDIRECTONAL_RELATIONSHIP=dict(local_attribute='hot_dog_id',database=DEFAULT_MATCHED_RELATION.database,schema=DEFAULT_MATCHED_RELATION.schema,relation=DEFAULT_MATCHED_RELATION.name, remote_attribute='hot_dog')
    TEST_DIRECTIONAL_RELATIONSHIP=dict(local_attribute='not_hotdog',database='PRODDATABASE',schema='SNOWSCHEMA',relation='COLDTABLE',remote_attribute='banana')



    tp.source_configs=dict()
    tp.source_configs['default_sampling']=dict(databases=[dict(name="(?i)^raw.*", schemas=[dict(name="RAWSCHEMA", relations=["(?i).*VIEW$"])])])  
    tp.source_configs['specified_relations']=[dict(database="(?i)^raw.*",schema="^SNOW.*", relation=".*Y",bidirectional=[TEST_BIDIRECTONAL_RELATIONSHIP],directional=[TEST_DIRECTIONAL_RELATIONSHIP])]

    tp.ANALYZE=True
    tp.source_adapter,tp.target_adapter=[mock.MagicMock() for _ in range(2)]

    mock_string=mock.MagicMock(return_value='')
    tp.source_adapter.predicate_constraint_statement=mock_string    
    tp.source_adapter.sample_statement_from_relation=mock_string

    tp.source_adapter.sample_method=BernoulliSample

    def _mock_full_catalog():
        tp.full_catalog=TEST_RELATIONS

    tp._load_full_catalog=mock.MagicMock(side_effect=_mock_full_catalog)
    tp.THREADS=4
    graphs=tp._build_uncompiled_graphs()
    for dag in graphs:
        assert isinstance(dag,networkx.Graph)
    
    for dag in graphs:
        assert dag.is_directed()

    for dag in graphs:
        for node in dag.nodes():
            if node == DEPENDENT_RELATION:
                assert SPECIFIED_RELATION in networkx.descendants(dag,node)

