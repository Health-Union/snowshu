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

    def create(self, 
               name:Union[str,None], 
               barf: bool) -> None:
        self.ANALYZE = False
        return self._execute(name=name,barf=barf)

    def analyze(self,barf:bool) -> None:
        self.ANALYZE = True
        return self._execute(barf=barf)

    def _execute(self,barf:bool=False, name:Union[str,None]=None) -> None:
        graph = SnowShuGraph()
        if name is not None:
            self.config.name = name

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
        if not self.ANALYZE:
            relations=[relation for graph in graphs for relation in graph.nodes]
            if self.config.source_profile.adapter.SUPPORTS_CROSS_DATABASE:
                logger.info('Creating x-database links in target...')
                self.config.target_profile.adapter.enable_cross_database(relations)
                logger.info('X-database enabled.')
                
            logger.info(f'Applying {self.config.source_profile.adapter.name} emulation functions to target...')
            for function in self.config.source_profile.adapter.SUPPORTED_FUNCTIONS:
                self.config.target_profile.adapter.create_function_if_available(function,relations) 
            logger.info('Emulation functions applied.')
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

