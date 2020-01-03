from tabulate import tabulate
import networkx as nx
from dataclasses import dataclass
from typing import Any,List
from snowshu.adapters.source_adapters.sample_methods import SampleMethod

@dataclass
class ReportRow:
    dot_notation:str
    population_size:int
    sample_size:int
    count_of_dependencies:str
    percent:Any
    percent_is_acceptable:bool

    def to_tuple(self)->list:
        return (self.dot_notation,
                self.population_size,
                self.sample_size,
                self.count_of_dependencies,
                self.percent,
                )

def graph_to_result_list(graphs:nx.Graph, sample_method:SampleMethod)->list:
    report=list()
    for graph in graphs:
        for relation in graph.nodes:
            deps = len(nx.ancestors(graph,relation))
            deps = " " if deps == 0 else str(deps)
            percent=0 if int(relation.population_size) < 1\
                           else round(100.0 * (relation.sample_size / relation.population_size))
            percent_is_acceptable=any((sample_method.is_acceptable(percent), (relation.unsampled and percent==100),))
            report.append(ReportRow(
                            relation.dot_notation,
                            relation.population_size,
                            relation.sample_size,
                            deps,
                            percent,
                            percent_is_acceptable))

    return report

def printable_result(report:List[ReportRow], analyze:str)->str:
    colors=dict(reset="\033[0m",
                red="\033[0;31m",
                green="\033[0;32m")
    printable=list()
    for row in report:
        formatter = 'green' if row.percent_is_acceptable else 'red'
        row.percent = f"{colors[formatter]}{row.percent}{colors['reset']}"
        printable.append(row.to_tuple())

    headers=('relation','population size','sample size','dependencies','aproximate %',)        
    column_alignment=('left','right','right','center','right',)
    title='ANALYZE' if analyze else 'RUN'
    message_top=f"\n\n{title} RESULTS:\n\n"
    return message_top + tabulate(printable,headers,colalign=column_alignment) +"\n"      

