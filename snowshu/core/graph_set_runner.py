import os
import shutil
import gc
from typing import List
from snowshu.configs import MAX_ALLOWED_ROWS
from snowshu.core.compile import RuntimeSourceCompiler
from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter
from snowshu.adapters.source_adapters.base_source_adapter import BaseSourceAdapter
import networkx as nx
from snowshu.logger import Logger, duration
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

logger = Logger().logger


@dataclass
class GraphExecutable:
    graph: nx.Graph
    source_adapter: BaseSourceAdapter
    target_adapter: BaseTargetAdapter
    analyze: bool


class GraphSetRunner:

    barf_output = 'snowshu_barf_output'
    def execute_graph_set(self,
                          graph_set: List[nx.Graph],
                          source_adapter: BaseSourceAdapter,
                          target_adapter: BaseTargetAdapter,
                          threads: int,
                          analyze: bool = False,
                          barf: bool = False) -> None:
        self.barf=barf
        if self.barf:
            shutil.rmtree(self.barf_output, ignore_errors=True)
            os.makedirs(self.barf_output)

        view_graph_set = [graph for graph in graph_set if graph.contains_views]
        table_graph_set = list(set(graph_set) - set(view_graph_set))

        def make_executables(graphs) -> List[GraphExecutable]:
            return [GraphExecutable(graph,
                                    source_adapter,
                                    target_adapter,
                                    analyze) for graph in graphs]

        start_time = time.time()

        # Tables need to come first to prevent deps deadlocks with views
        for graphs in [table_graph_set, view_graph_set]:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                for executable in make_executables(graphs):
                    executor.submit(self._traverse_and_execute,
                                    executable, start_time)

    def _traverse_and_execute(
            self, executable: GraphExecutable, start_time: int) -> None:
        try:
            logger.debug(
                f"Executing graph with {len(executable.graph)} relations in it...")
            for i, relation in enumerate(
                    nx.algorithms.dag.topological_sort(executable.graph)):
                relation.population_size=executable.source_adapter.scalar_query(
                                         executable.source_adapter.population_count_statement(relation))
                logger.info(
                    f'Executing graph {i+1} of {len(executable.graph)} source query for relation {relation.dot_notation}...')

                relation.sampling.prepare(relation,
                                          executable.source_adapter)
                relation = RuntimeSourceCompiler.compile_queries_for_relation(
                    relation, executable.graph, executable.source_adapter, executable.analyze)

                if executable.analyze:
                    if relation.is_view:
                        relation.population_size = "N/A"
                        relation.sample_size = "N/A"
                        logger.info(
                            f'Relation {relation.dot_notation} is a view, skipping.')
                    else:
                        result = [
                            row for row in executable.source_adapter.check_count_and_query(
                                relation.compiled_query,
                                MAX_ALLOWED_ROWS).itertuples()][0]
                        relation.population_size = result.population_size
                        relation.sample_size = result.sample_size
                        logger.info(
                            f'Analysis of relation {relation.dot_notation} completed in {duration(start_time)}.')
                else:
                    executable.target_adapter.create_database_if_not_exists(
                        relation.database)
                    executable.target_adapter.create_schema_if_not_exists(
                        relation.database, relation.schema)
                    if relation.is_view:
                        logger.info(
                            f'Retrieving DDL statement for view {relation.dot_notation} in source...')
                        relation.population_size = "N/A"
                        relation.sample_size = "N/A"
                        try:
                            relation.view_ddl = executable.source_adapter.scalar_query(relation.compiled_query)
                        except Exception:
                            raise SystemError(
                                f'Failed to extract DDL statement: {relation.compiled_query}')
                        logger.info(
                            f'Successfully extracted DDL statement for view {relation.quoted_dot_notation}')
                    else:
                        logger.info(
                            f'Retrieving records from source {relation.dot_notation}...')
                        try:
                            relation.data = executable.source_adapter.check_count_and_query(
                                relation.compiled_query, MAX_ALLOWED_ROWS)
                        except Exception:
                            raise SystemError(
                                f'Failed execution of extraction sql statement: {relation.compiled_query}')

                        relation.sample_size = len(relation.data)
                        logger.info(
                            f'{relation.sample_size} records retrieved for relation {relation.dot_notation}.')

                    logger.info(
                        f'Inserting relation {relation.quoted_dot_notation} into target...')
                    try:
                        executable.target_adapter.create_and_load_relation(
                            relation)
                    except Exception as e:
                        raise SystemError(
                            f'Failed to load relation {relation.quoted_dot_notation} into target: {e}')

                    logger.info(
                        f'Done replication of relation {relation.dot_notation} in {duration(start_time)}.')
                    relation.target_loaded = True
                relation.source_extracted = True
                logger.info(
                    f'population:{relation.population_size}, sample:{relation.sample_size}')
                if self.barf:
                   with open(os.path.join(self.barf_output,f'{relation.dot_notation}.sql'),'w') as f:
                        f.write(relation.compiled_query) 
            try:
                for relation in executable.graph.nodes:
                    del relation.data
            except AttributeError:
                pass
            gc.collect()
        except Exception as e:
            logger.error(f'failed with error of type {type(e)}: {str(e)}')
            raise e
