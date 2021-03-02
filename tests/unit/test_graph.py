import pytest
import mock
from tests.conftest import CONFIGURATION
from io import StringIO
import yaml
import copy
import networkx as nx
from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.graph import SnowShuGraph
from snowshu.samplings.samplings import DefaultSampling,BruteForceSampling
from snowshu.core.models import materializations as mz
from snowshu.core.models import Relation

def test_graph_builds_dags_correctly(stub_graph_set):
    shgraph = SnowShuGraph()
    _, vals = stub_graph_set

    full_catalog = [vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]

    graph = nx.Graph()
    graph.add_nodes_from(full_catalog)
    shgraph.graph = graph

    for sub in shgraph.get_graphs():
        assert isinstance(sub, nx.DiGraph)


def test_graph_allows_upstream_wildcards(stub_graph_set):
    shgraph = SnowShuGraph()
    _, vals = stub_graph_set

    vals.upstream_relation.database = vals.downstream_relation.database
    vals.upstream_relation.schema = vals.downstream_relation.schema
    full_catalog = [vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]
    config_dict = copy.deepcopy(CONFIGURATION)

    config_dict['source']['specified_relations'] = [dict(relation=vals.downstream_relation.name,
                                                         database=vals.downstream_relation.database,
                                                         schema=vals.downstream_relation.schema,
                                                         unsampled=False,
                                                         relationships=dict(directional=[],
                                                                            bidirectional=[dict(relation=vals.upstream_relation.name,
                                                                                                database='',
                                                                                                schema='',
                                                                                                local_attribute=vals.downstream_relation.attributes[
                                                                                                    0].name,
                                                                                                remote_attribute=vals.upstream_relation.attributes[0].name)]))]

    config = ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))

    modified_graph = shgraph._apply_specifications(
        config, nx.DiGraph(), full_catalog)

    assert (vals.upstream_relation, vals.downstream_relation,
            ) in modified_graph.edges


def test_unsampled(stub_graph_set):
    shgraph = SnowShuGraph()

    _, vals = stub_graph_set

    full_catalog = [vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]

    config_dict = copy.deepcopy(CONFIGURATION)
    config_dict['source']['specified_relations'] = [dict(relation=vals.iso_relation.name,
                                                         database=vals.iso_relation.database,
                                                         schema=vals.iso_relation.schema,
                                                         unsampled=True)]

    config = ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))
    assert vals.iso_relation.unsampled == False

    modified_graph = shgraph._apply_specifications(
        config, nx.DiGraph(), full_catalog)
    modified_graph=shgraph._apply_specifications(config,nx.DiGraph(),full_catalog)       
 
    assert vals.iso_relation.unsampled==True

def test_sets_outliers(stub_graph_set):
    shgraph=SnowShuGraph()

    _,vals = stub_graph_set

    full_catalog=[  vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]

    config_dict=copy.deepcopy(CONFIGURATION)
    config_dict['source']['include_outliers']=True
    config_dict['source']['max_number_of_outliers']=1000

    config=ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))
    
    with mock.MagicMock() as adapter_mock:
        adapter_mock.build_catalog.return_value = full_catalog
        config.source_profile.adapter = adapter_mock
        _ = shgraph.build_graph(config)

    assert vals.iso_relation.include_outliers==True
    assert vals.iso_relation.max_number_of_outliers==1000


def test_no_duplicates(stub_graph_set):
    shgraph=SnowShuGraph()

    _,vals = stub_graph_set

    full_catalog=[  vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]

    config_dict=copy.deepcopy(CONFIGURATION)

    config=ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))
    
    with mock.MagicMock() as adapter_mock:
        adapter_mock.build_catalog.return_value = full_catalog
        config.source_profile.adapter = adapter_mock
        shgraph.build_graph(config)
        graphs = shgraph.get_graphs()

    all_nodes=[node for graph in graphs for node in graph.nodes]
    assert len(set(all_nodes)) == len(all_nodes)

def test_split_dag_to_parallel():
    shgraph=SnowShuGraph()
    dag=nx.DiGraph()
    dag.add_edges_from([(1,2,),(1,4,),(2,3,),(5,6,)])
    split=shgraph._split_dag_for_parallel(dag)
    
    assert set([frozenset(val) for val in split]) == set([frozenset([1,2,4,3]),frozenset([5,6])])


def test_sets_only_existing_adapters():
    shgraph=SnowShuGraph()
    
    test_relation=Relation(
                 database='SNOWSHU_DEVELOPMENT',
                 schema='SOURCE_SYSTEM',
                 name='ORDER_ITEMS',
                 materialization=mz.TABLE,
                 attributes=[]
                    )
    test_relation.include_outliers, test_relation.unsampled = [False for _ in range(2)]   
    test_relation.sampling=DefaultSampling()
    config_dict=copy.deepcopy(CONFIGURATION)
    config_dict['preserve_case'] = True
    config_dict['source']['specified_relations'][1]['sampling']='lucky_guess'
    with pytest.raises(AttributeError):
        config=ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))

    assert isinstance(test_relation.sampling,DefaultSampling)
    config_dict['source']['specified_relations'][1]['sampling']='brute_force'
    config=ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))
    
    assert isinstance(shgraph._set_overriding_params_for_node(test_relation,config).sampling,
                      BruteForceSampling)
