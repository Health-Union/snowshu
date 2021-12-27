import gc
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Tuple

import networkx as nx

from snowshu.adapters.source_adapters.base_source_adapter import \
    BaseSourceAdapter
from snowshu.adapters.target_adapters.base_target_adapter import \
    BaseTargetAdapter
from snowshu.configs import MAX_ALLOWED_ROWS
from snowshu.core.compile import RuntimeSourceCompiler
from snowshu.logger import Logger, duration

logger = Logger().logger


@dataclass
class GraphExecutable:
    graph: nx.Graph
    source_adapter: BaseSourceAdapter
    target_adapter: BaseTargetAdapter
    analyze: bool


class GraphSetRunner:

    barf_output = 'snowshu_barf_output'

    def __init__(self):
        self.barf = None

    def execute_graph_set(self,     # noqa pylint: disable=too-many-arguments
                          graph_set: Tuple[nx.Graph],
                          source_adapter: BaseSourceAdapter,
                          target_adapter: BaseTargetAdapter,
                          threads: int,
                          analyze: bool = False,
                          barf: bool = False) -> None:
        """ Processes the given graphs in parallel based on the provided adapters

            Args:
                graph_set (list): list of graphs to process
                source_adapter (BaseSourceAdapter): source adapter for the relations
                target_adapter (BaseTargetAdapter): target adapter for the relations
                threads (int): number of threads to use for parallelization
                analyze (bool): whether to run analyze or actually transfer the sampled data
                barf (bool): whether to dump diagnostic files to disk
        """
        retry_count = 3
        self.barf = barf
        if self.barf:
            shutil.rmtree(self.barf_output, ignore_errors=True)
            os.makedirs(self.barf_output)

        view_graph_set = [graph for graph in graph_set if graph.contains_views]
        table_graph_set = list(set(graph_set) - set(view_graph_set))

        # Tables need to come first to prevent deps deadlocks with views
        for graphs in [table_graph_set, view_graph_set]:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                if graphs:
                    executables = [GraphExecutable(graph,
                                                   source_adapter,
                                                   target_adapter,
                                                   analyze)
                                   for graph in graphs]
                    self.process_executables(executables,
                                             executor,
                                             retry_count)

    def process_executables(self,
                            executables,
                            executor,
                            retries,
                            wait_time=30):
        re_executables = []

        def execute_with_retry(executables):
            error = None
            re_executables.clear()
            futures = [executor.submit(self._traverse_and_execute, executable)
                       for executable in executables]
            while futures:
                time.sleep(wait_time)
                for future, executable in zip(futures.copy(), executables.copy()):
                    if future.done():
                        futures.remove(future)
                        executables.remove(executable)
                        if exception := future.exception():
                            error = future.result
                            logger.warning("Concurrent thread finished work with exception:\n%s: %s",
                                           str(exception.__class__),
                                           str(exception))
                            re_executables.append(executable)
            return error

        while error := execute_with_retry(executables):
            if retries < 1:
                logger.error("Failed because '%i' executables can't be finished successfully:\n%s",
                             len(re_executables), str(re_executables))
                error()
            retries -= 1
            executables = re_executables.copy()


    def _traverse_and_execute(self, executable: GraphExecutable) -> None:   # noqa mccabe: disable=MC0001
        """ Processes a single graph and loads the data into the replica if required

            To save memory after processing, the loaded dataframes are deleted, and
            garbage collection manually called.

            Args:
                executable (GraphExecutable): object that contains all of the necessary info for
                    executing a sample and loading it into the target
        """
        start_time = time.time()
        if self.barf:
            with open(os.path.join(self.barf_output, f'{[n for n in executable.graph.nodes][0].dot_notation}.component'), 'wb') as cmp_file:  # noqa pylint: disable=unnecessary-comprehension
                nx.write_multiline_adjlist(executable.graph, cmp_file)
        try:
            logger.debug(
                f"Executing graph with {len(executable.graph)} relations in it...")
            for i, relation in enumerate(
                    nx.algorithms.dag.topological_sort(executable.graph)):
                relation.population_size = executable.source_adapter.scalar_query(
                    executable.source_adapter.population_count_statement(relation))
                logger.info(f'Executing source query for relation {relation.dot_notation} '
                            f'({i+1} of {len(executable.graph)} in graph)...')

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
                        result = executable.source_adapter.check_count_and_query(relation.compiled_query,
                                                                                 MAX_ALLOWED_ROWS,
                                                                                 relation.unsampled).iloc[0]
                        relation.population_size = result.population_size
                        relation.sample_size = result.sample_size
                        logger.info(
                            f'Analysis of relation {relation.dot_notation} completed in {duration(start_time)}.')
                else:
                    executable.target_adapter.create_database_if_not_exists(
                        relation.quoted(relation.database))
                    executable.target_adapter.create_schema_if_not_exists(
                        relation.quoted(relation.database),
                        relation.quoted(relation.schema))
                    if relation.is_view:
                        logger.info(
                            f'Retrieving DDL statement for view {relation.dot_notation} in source...')
                        relation.population_size = "N/A"
                        relation.sample_size = "N/A"
                        try:
                            relation.view_ddl = executable.source_adapter.scalar_query(
                                relation.compiled_query)
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
                                relation.compiled_query, MAX_ALLOWED_ROWS, relation.unsampled)
                        except Exception as exc:
                            raise SystemError(
                                f'Failed execution of extraction sql statement: {relation.compiled_query} {exc}')

                        relation.sample_size = len(relation.data)
                        logger.info(
                            f'{relation.sample_size} records retrieved for relation {relation.dot_notation}.')

                    logger.info(
                        f'Inserting relation {relation.quoted_dot_notation} into target...')
                    try:
                        executable.target_adapter.create_and_load_relation(
                            relation)
                    except Exception as exc:
                        raise SystemError(
                            f'Failed to load relation {relation.quoted_dot_notation} into target: {exc}')

                    logger.info(
                        f'Done replication of relation {relation.dot_notation} in {duration(start_time)}.')
                    relation.target_loaded = True
                relation.source_extracted = True
                logger.info(
                    f'population:{relation.population_size}, sample:{relation.sample_size}')
                if self.barf:
                    with open(os.path.join(self.barf_output, f'{relation.dot_notation}.sql'), 'w') as barf_file:
                        barf_file.write(relation.compiled_query)
            try:
                for relation in executable.graph.nodes:
                    del relation.data
            except AttributeError:
                pass
            gc.collect()
        except Exception as exc:
            logger.error(f'failed with error of type {type(exc)}: {str(exc)}')
            raise exc
