from tabulate import tabulate
import networkx as nx
from dataclasses import dataclass
from typing import Any, List, Union
from snowshu.logger import Logger
logger = Logger().logger


@dataclass
class ReportRow:
    dot_notation: str
    population_size: Union[int, str]
    target_sample_size:Union[int,str]
    final_sample_size: Union[int, str]
    count_of_dependencies: str
    percent_to_target: Any
    percent_is_acceptable: bool

    def to_tuple(self) -> list:
        return (self.dot_notation,
                self.population_size,
                self.target_sample_size,
                self.final_sample_size,
                self.count_of_dependencies,
                self.percent_to_target,
                )


def graph_to_result_list(graphs: nx.Graph) -> list:
    report = list()
    for graph in graphs:
        try:
            for relation in graph.nodes:
                deps = len(nx.ancestors(graph, relation))
                deps = " " if deps == 0 else str(deps)
                target_sample_size=relation.population_size if\
                    (relation.unsampled or relation.population_size < relation.sampling.size)\
                    else relation.sampling.size
                
                if isinstance(relation.population_size, str):
                    percent = "N/A"
                elif int(relation.population_size) < 1:
                    percent = 0
                else:
                    percent = int(round(
                        100.0 * (relation.sample_size / target_sample_size)))

                percent_is_acceptable = True if isinstance(percent, str) else abs(percent-100) <= 5
                report.append(ReportRow(
                    relation.dot_notation,
                    relation.population_size,
                    target_sample_size,
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
        row.percent_to_target = f"{colors[formatter]}{row.percent_to_target} %{colors['reset']}"
        printable.append(row.to_tuple())

    headers = ('relation', 'population size', 'target sample size',
               'final sample size',
               'dependencies', 'aproximate % to target',)
    column_alignment = ('left', 'right', 'right', 'right', 'center', 'right',)
    title = 'ANALYZE' if analyze else 'RUN'
    message_top = f"\n\n{title} RESULTS:\n\n"
    return message_top + \
        tabulate(printable, headers, colalign=column_alignment) + "\n"


def format_set_of_available_images(imageset:iter)->str:
    """Transforms an iterable of tuples into a response pretty printed.

    Args:
        imageset: a tuple or ordered iterable in format (image name, last modified datetime, source adapter, target adapter). 
    Returns:
        formatted color output.
    """
    headers= ('Replica name',
              'modified',
              'source',
              'replica engine',
              'docker image',)

    return "\n\nAVAILABLE IMAGES:\n\n" + tabulate(imageset,headers)
