import pytest
import networkx as nx
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
