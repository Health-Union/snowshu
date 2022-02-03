import snowshu.core.models.materializations as mz
import snowshu.core.printable_result as pr
from snowshu.samplings.samplings import DefaultSampling


def generate_stub_complete_graph(stub_graph_set):
    graph_list, _ = stub_graph_set
    view_list = []
    for graph in graph_list:
        for rel in graph:
            # set mocked sample
            if rel.is_view:
                rel.population_size = "N/A"
                rel.sample_size = "N/A"
                view_list.append(rel.dot_notation)
            else:
                rel.population_size = 1000
                rel.sample_size = 10
                # set and prepare sampling
                rel.sampling = DefaultSampling()
                rel.sampling.prepare(rel, None)

    return graph_list, view_list


def test_graph_to_list(stub_graph_set):
    graph_list, view_list = generate_stub_complete_graph(stub_graph_set)

    report = pr.graph_to_result_list(graph_list)

    assert isinstance(report, list)
    for row in report:
        assert isinstance(row, pr.ReportRow)
        if row.dot_notation in view_list:
            assert row.population_size == 'N/A'
            assert row.target_sample_size == 'N/A'
            assert row.final_sample_size == 'N/A'
            assert row.count_of_dependencies in (' ')
            assert row.percent_to_target == 'N/A'
            assert row.percent_is_acceptable == True
        else:
            assert row.population_size == 1000
            assert row.target_sample_size == 1000
            assert row.final_sample_size == 10
            assert row.count_of_dependencies in (' ', '1')  # some relations had a dependency
            assert row.percent_to_target == 1
            assert row.percent_is_acceptable == False
