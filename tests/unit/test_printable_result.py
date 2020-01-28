import pytest
import snowshu.core.printable_result as pr


@pytest.mark.skip
def test_graph_to_list(stub_graph_set):
    graphs, _ = stub_graph_set
    for rel in graphs:
        rel.populuation_size = 1000
        rel.sample_size = 10

    report = pr.graph_to_result_list(graphs)

    assert isinstance(report, list)
    for row in report:
        assert isinstance(row, pr.ReportRow)
        assert row.percent == 10
        assert row.percent_is_acceptable
