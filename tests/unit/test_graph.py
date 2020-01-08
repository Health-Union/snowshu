import pytest
from tests.conftest import CONFIGURATION
import copy
import networkx as nx
from snowshu.core.configuration_parser import Configuration
from snowshu.core.graph import SnowShuGraph

def test_graph_builds_dags_correctly(stub_graph_set):
    shgraph=SnowShuGraph()
    _,vals = stub_graph_set

    full_catalog=[  vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]

    graph=nx.Graph()
    graph.add_nodes_from(full_catalog)
    shgraph.graph=graph

    for sub in shgraph.get_graphs():
        assert isinstance(sub,nx.DiGraph)

def test_graph_allows_upstream_wildcards(stub_graph_set):
    shgraph=SnowShuGraph()
    _,vals = stub_graph_set

    vals.upstream_relation.database=vals.downstream_relation.database
    vals.upstream_relation.schema=vals.downstream_relation.schema
    full_catalog=[  vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]
    config_dict=copy.deepcopy(CONFIGURATION)
    
    config_dict['source']['specified_relations'] = dict(name=vals.downstream_relation.name,
                                 database=vals.downstream_relation.database,
                                 schema=vals.downstream_relation.schema,
                                 unsampled=False,
                                 relationships=dict(directional=[],
                                                    bidirectional=[dict(relation=vals.upstream_relation.name,
                                                                        database='',
                                                                        schema='',
                                                                        local_attribute=vals.downstream_relation.attributes[0].name,
                                                                        remote_attribute=vals.upstream_relation.attributes[0].name)]))
       
    
    
    config=ConfigurationParser.from_file_or_path(StringIO(yaml.dump(config_dict)))

    modified_graph=shgraph._apply_specifications([config],nx.DiGraph(),full_catalog)       
    
    assert (vals.upstream_relation,vals.downstream_relation,) in modified_graph.edges   


def test_unsampled(stub_graph_set):
    shgraph=SnowShuGraph()

    _,vals = stub_graph_set

    full_catalog=[  vals.iso_relation,
                    vals.view_relation,
                    vals.downstream_relation,
                    vals.upstream_relation,
                    vals.birelation_left,
                    vals.birelation_right]

    config=[dict(name=vals.iso_relation.name,
                 database=vals.iso_relation.database,
                 schema=vals.iso_relation.schema,
                 unsampled=True)]

    assert vals.iso_relation.unsampled==False    

    modified_graph=shgraph._apply_specifications(config,nx.DiGraph(),full_catalog)       
 
    assert vals.iso_relation.unsampled==True
