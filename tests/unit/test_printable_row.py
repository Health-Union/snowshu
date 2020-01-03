import pytest
from snowshu.adapters.source_adapters.sample_methods import BernoulliSample
from snowshu.core import printable_row as pr


def test_graph_to_list(stub_graph_set):
    graph,_=stub_graph_set
    for graph in graphs:
        for rel in graph.nodes:
            rel.populuation_size=1000
            rel.sample_size=10
           
    report=pr.graph_to_result_list(graph, BernoulliSample(probability=10))

    assert isinstance(report,list)
    for row in report:
        assert isinstance(row,pr.ReportRow)
        assert row.percent == 10
        assert row.percent_is_acceptable 
