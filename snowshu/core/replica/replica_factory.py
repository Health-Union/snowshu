import time
from typing import TextIO, List, Union
import networkx
from snowshu.core.graph import SnowShuGraph
from snowshu.core.catalog import Catalog
from snowshu.logger import Logger, duration
from snowshu.core.graph_set_runner import GraphSetRunner
from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.printable_result import graph_to_result_list, printable_result
logger = Logger().logger


class ReplicaFactory:

    def __init__(self):
        self._credentials = dict()

    def run(self, barf: bool) -> None:
        self.ANALYZE = False
        return self._execute(barf)

    def analyze(self,barf:bool) -> None:
        self.ANALYZE = True
        return self._execute(barf=barf)

    def _execute(self,barf:bool) -> None:
        graph = SnowShuGraph()
        graph.build_graph(self.config,
                          Catalog.load_full_catalog(
                                    self.config.source_profile.adapter, 
                                    self.config.threads))
        graphs = graph.get_graphs()    
        if len(graphs) < 1:
            return "No relations found per provided replica configuration, exiting."

        self.config.target_profile.adapter.initialize_replica(self.config.source_profile.name)
        runner = GraphSetRunner()
        runner.execute_graph_set(graphs,
                                 self.config.source_profile.adapter,
                                 self.config.target_profile.adapter,
                                 threads=self.config.threads,
                                 analyze=self.ANALYZE,
                                 barf=barf)

        self.config.target_profile.adapter.finalize_replica()

        return printable_result(
                graph_to_result_list(graphs),
                self.ANALYZE)

    def load_config(self, config: Union['Path', str, TextIO]):
        """does all the initial work to make the resulting ReplicaFactory
        object usable."""
        logger.info('Loading credentials...')
        start_timer = time.time()
        self.config = ConfigurationParser.from_file_or_path(config)
        logger.info(f'Credentials loaded in {duration(start_timer)}.')

