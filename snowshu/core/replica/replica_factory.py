import time
from pathlib import Path
from typing import TextIO, Union

from snowshu.core.configuration_parser import (Configuration,
                                               ConfigurationParser)
from snowshu.core.graph import SnowShuGraph
from snowshu.core.graph_set_runner import GraphSetRunner
from snowshu.core.printable_result import (graph_to_result_list,
                                           printable_result)
from snowshu.logger import Logger, duration

logger = Logger().logger


class ReplicaFactory:

    def __init__(self):
        self._credentials = dict()
        self.config: Configuration = None
        self.run_analyze: bool = None

    def create(self,
               name: Union[str, None],
               barf: bool) -> None:
        self.run_analyze = False
        return self._execute(name=name, barf=barf)

    def analyze(self, barf: bool) -> None:
        self.run_analyze = True
        return self._execute(barf=barf)

    def _execute(self,
                 barf: bool = False,
                 name: Union[str, None] = None) -> None:
        graph = SnowShuGraph()
        if name is not None:
            self.config.name = name

        graph.build_graph(self.config)
        graphs = graph.get_graphs()
        if len(graphs) < 1:
            return "No relations found per provided replica configuration, exiting."

        self.config.target_profile.adapter.initialize_replica(
            self.config.source_profile.name)
        runner = GraphSetRunner()
        runner.execute_graph_set(graphs,
                                 self.config.source_profile.adapter,
                                 self.config.target_profile.adapter,
                                 threads=self.config.threads,
                                 analyze=self.run_analyze,
                                 barf=barf)
        if not self.run_analyze:
            relations = [
                relation for graph in graphs for relation in graph.nodes]
            if self.config.source_profile.adapter.SUPPORTS_CROSS_DATABASE:
                logger.info('Creating x-database links in target...')
                self.config.target_profile.adapter.enable_cross_database(
                    relations)
                logger.info('X-database enabled.')

            logger.info(
                'Applying %s emulation functions to target...',
                self.config.source_profile.adapter.name)
            for function in self.config.source_profile.adapter.SUPPORTED_FUNCTIONS:
                self.config.target_profile.adapter.create_function_if_available(
                    function, relations)
            logger.info('Emulation functions applied.')
            self.config.target_profile.adapter.finalize_replica()

        return printable_result(
            graph_to_result_list(graphs),
            self.run_analyze)

    def load_config(self, config: Union[Path, str, TextIO]):
        """does all the initial work to make the resulting ReplicaFactory
        object usable."""
        logger.info('Loading configuration...')
        start_timer = time.time()
        self.config = ConfigurationParser().from_file_or_path(config)
        logger.info('Configuration loaded in %s.', duration(start_timer))
