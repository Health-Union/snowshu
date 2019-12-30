from snowshu.core.models.relation import Relation
import pytest
import mock
from snowshu.adapters.source_adapters.sample_methods import BernoulliSample 
from snowshu.core.replica import Replica
from snowshu.core.models.materializations import TABLE,VIEW
from snowshu.core.models.attribute import Attribute
import snowshu.core.models.data_types as dt
import networkx

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
    tp.source_configs['specified_relations']=[dict(database="(?i)^raw.*",schema="^SNOW.*", relation=".*Y",relationships=dict(bidirectional=[TEST_BIDIRECTONAL_RELATIONSHIP],directional=[TEST_DIRECTIONAL_RELATIONSHIP]))]

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
            if node == SPECIFIED_RELATION:
                assert len(networkx.ancestors(dag,node)) > 0 
