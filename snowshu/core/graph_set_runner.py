import gc
from typing import List
from snowshu.utils import MAX_ALLOWED_ROWS
from snowshu.target_adapters.base_target_adapter import BaseTargetAdapter
from snowshu.source_adapters.base_source_adapter import BaseSourceAdapter
import networkx as nx
from snowshu.logger import Logger, duration
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

logger=Logger().logger

@dataclass
class GraphExecutable:
    graph:nx.Graph
    source_adapter:BaseSourceAdapter
    target_adapter:BaseTargetAdapter
    analyze:bool
        

class GraphSetRunner:

    def execute_graph_set(  self,
                            graph_set:List[nx.Graph],
                            source_adapter:BaseSourceAdapter,
                            target_adapter:BaseTargetAdapter,
                            threads:int,
                            analyze:bool=False)->None:
        
        executables=[GraphExecutable(   graph,
                                        source_adapter,
                                        target_adapter,
                                        analyze) for graph in graph_set]

        start_time=time.time()
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for executable in executables:
                executor.submit(self._traverse_and_execute,executable,start_time)
    
    def _traverse_and_execute(self,executable:GraphExecutable,start_time:int)->None:
        try:
            logger.debug(f"Executing graph with {len(executable.graph)} relations in it...")
            for i,relation in enumerate(nx.algorithms.dag.topological_sort(executable.graph)):
                logger.debug(f'Executing graph {i+1} of {len(executable.graph)} source query for relation {relation.dot_notation}...')

                if executable.analyze:
                    result=[row for row in executable.source_adapter.check_count_and_query(relation.compiled_query,MAX_ALLOWED_ROWS).itertuples()][0]
                    relation.population_size=result.population_size
                    relation.sample_size=result.sample_size
                    logger.info(f'Analysis of relation {relation.dot_notation} completed in {duration(start_time)}.')        
                else:
                    relation.data=executable.source_adapter.check_count_and_query(relation.compiled_query,MAX_ALLOWED_ROWS)
                    logger.info(f'{len(relation.data)} records retrieved. Inserting into target...')
                    executable.target_adapter.create_relation(relation)
                    executable.target_adapter.insert_into_relation(relation)
                    logger.info(f'Done replication of relation {relation.dot_notation} in {duration(start_time)}.')        
                    relation.target_loaded=True
                relation.source_extracted=True
            try:
                for relation in executable.graph.nodes:
                    del relation.data
            except AttributeError:
                pass
            gc.collect()       
        except Exception as e:
            logger.error(e)
            raise e
