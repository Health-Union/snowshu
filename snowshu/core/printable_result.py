from tabulate import tabulate
import networkx as nx
from dataclasses import dataclass
from typing import Any, List, Union
from snowshu.core.sample_methods import SampleMethod
from snowshu.logger import Logger
logger = Logger().logger


@dataclass
class ReportRow:
    dot_notation: str
    population_size: Union[int, str]
    sample_size: Union[int, str]
    count_of_dependencies: str
    percent: Any
    percent_is_acceptable: bool

    def to_tuple(self) -> list:
        return (self.dot_notation,
                self.population_size,
                self.sample_size,
                self.count_of_dependencies,
                self.percent,
                )


def graph_to_result_list(graphs: nx.Graph,
                         sample_method: SampleMethod) -> list:
    report = list()
    for graph in graphs:
        try:
            for relation in graph.nodes:
                deps = len(nx.ancestors(graph, relation))
                deps = " " if deps == 0 else str(deps)
                if isinstance(relation.population_size, str):
                    percent = "N/A"
                elif int(relation.population_size) < 1:
                    percent = 0
                else:
                    percent = round(
                        100.0 * (relation.sample_size / relation.population_size))

                percent_is_acceptable = True if isinstance(percent, str) else any(
                    (sample_method.is_acceptable(percent), (relation.unsampled and percent == 100),))
                report.append(ReportRow(
                    relation.dot_notation,
                    relation.population_size,
                    relation.sample_size,
                    deps,
                    percent,
                    percent_is_acceptable))
        except Exception as e:
            message = f"failure in building row for relation {relation.dot_notation} : {e}"
            logger.critical(message)
            raise ValueError(message)
    return report


def printable_result(report: List[ReportRow], analyze: str) -> str:
    colors = dict(reset="\033[0m",
                  red="\033[0;31m",
                  green="\033[0;32m")
    printable = list()
    for row in report:
        formatter = 'green' if row.percent_is_acceptable else 'red'
        row.percent = f"{colors[formatter]}{row.percent}{colors['reset']}"
        printable.append(row.to_tuple())

    headers = ('relation', 'population size', 'sample size',
               'dependencies', 'aproximate %',)
    column_alignment = ('left', 'right', 'right', 'center', 'right',)
    title = 'ANALYZE' if analyze else 'RUN'
    message_top = f"\n\n{title} RESULTS:\n\n"
    return message_top + \
        tabulate(printable, headers, colalign=column_alignment) + "\n"
