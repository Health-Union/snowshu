import gc
import os
import shutil
import time
import threading
from threading import Lock
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Tuple, Set
import logging

import networkx as nx

from snowshu.adapters.base_sql_adapter import BaseSQLAdapter
from snowshu.adapters.source_adapters.base_source_adapter import \
    BaseSourceAdapter
from snowshu.adapters.target_adapters.base_target_adapter import \
    BaseTargetAdapter
from snowshu.core import utils
from snowshu.core.compile import RuntimeSourceCompiler
from snowshu.logger import duration

logger = logging.getLogger(__name__)


@dataclass
class GraphExecutable:
    graph: nx.Graph
    source_adapter: BaseSourceAdapter
    target_adapter: BaseTargetAdapter
    analyze: bool


class GraphSetRunner:

    barf_output = 'snowshu_barf_output'
    schemas_lock: threading.Lock = Lock()
    schemas: Set[str] = set()
    uuid: str = utils.generate_unique_uuid()

    def __init__(self):
        self.barf = None

    def execute_graph_set(self,     # noqa pylint: disable=too-many-arguments
                          graph_set: Tuple[nx.Graph],
                          source_adapter: BaseSourceAdapter,
                          target_adapter: BaseTargetAdapter,
                          threads: int,
                          retry_count: int,
                          analyze: bool = False,
                          barf: bool = False) -> None:
        """ Processes the given graphs in parallel based on the provided adapters

            Args:
                graph_set (list): list of graphs to process
                source_adapter (BaseSourceAdapter): source adapter for the relations
                target_adapter (BaseTargetAdapter): target adapter for the relations
                threads (int): number of threads to use for parallelization
                retry_count (int): number of times to retry failed query
                analyze (bool): whether to run analyze or actually transfer the sampled data
                barf (bool): whether to dump diagnostic files to disk
        """
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
                            executables: List[GraphExecutable],
                            executor: ThreadPoolExecutor,
                            retries: int) -> None:
        """
        Executes a list of GraphExecutable tasks concurrently using a ThreadPoolExecutor.
        If any task fails due to an exception, it is retried a specified number of times.

        Args:
            executables (List[GraphExecutable]): The list of tasks to be executed.
            executor (ThreadPoolExecutor): The executor to run the tasks.
            retries (int): The number of times to retry failed tasks.

        Returns:
            None

        Raises:
            Exception: If a task fails after the specified number of retries,
                    an exception is logged and the function returns None.
        """
        while retries >= 0:
            futures = {
                executor.submit(self._traverse_and_execute, executable): executable
                for executable in executables
            }
            completed, _ = concurrent.futures.wait(
                futures.keys(), timeout=60, return_when=concurrent.futures.FIRST_EXCEPTION
            )
            re_executables = []

            for future in completed:
                executable = futures[future]
                if exception := future.exception():
                    logger.warning("Concurrent thread finished work with exception:\n%s: %s",
                                   str(exception.__class__),
                                   str(exception))
                    re_executables.append(executable)

            if not re_executables:
                return  # Success

            logging.error("Failed because '%i' executables can't be finished successfully:\n%s",
                          len(re_executables), str(re_executables))
            executables = re_executables
            retries -= 1
        logging.error("Max retries reached. Some executables failed.")

    def _generate_schemas_if_necessary(self, adapter: BaseSQLAdapter, name: str, database: str) -> None:
        """
        Helper function needed due to multi threading. We need to generate
        schemas in database only if they don't already exists there. Due to
        multi-threading we need to set up a lock on self.schema variable
        so we don't create the same table in two separate threads.

        Args:
            name (str): Schema name to check if exists and generate if not
            database (str): Database name where to check schema presence
            adapter (BaseSQLAdapter): object that contains method necessary
                to generate schema
        """
        with self.schemas_lock:
            if name not in self.schemas:
                adapter.generate_schema(name, database)
                self.schemas.add(name)

    def _traverse_and_execute(self, executable: GraphExecutable) -> None:  # noqa: pylint: disable=too-many-statements
        """ Processes a single graph and loads the data into the replica if required

            To save memory after processing, the loaded dataframes are deleted, and
            garbage collection manually called.

            Args:
                executable (GraphExecutable): object that contains all of the necessary info for
                    executing a sample and loading it into the target
        """
        start_time = time.time()
        if self.barf:
            with open(os.path.join(self.barf_output,
                                   f'{[n for n in executable.graph.nodes][0].dot_notation}.component'),  # noqa: pylint: disable=unnecessary-comprehension
                                   'wb'
                                ) as cmp_file:
                nx.write_multiline_adjlist(executable.graph, cmp_file)

        try:
            logger.debug(
                "Executing graph with %s relations in it...", len(executable.graph)
            )
            for i, relation in enumerate(
                nx.algorithms.dag.topological_sort(executable.graph), start=1
            ):
                unique_schema_name = "_".join([relation.schema, self.uuid])
                self._generate_schemas_if_necessary(
                    executable.source_adapter, unique_schema_name, relation.temp_database
                )

                relation.population_size = executable.source_adapter.scalar_query(
                    executable.source_adapter.population_count_statement(relation)
                )
                logger.info(
                    "Executing source query for relation %s (%s of %s in graph)...",
                    relation.dot_notation,
                    i,
                    len(executable.graph),
                )

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
                                                                                 relation.sampling.max_allowed_rows,
                                                                                 relation.unsampled).iloc[0]
                        relation.population_size = result.population_size
                        relation.sample_size = result.sample_size
                        logger.info(
                            f'Analysis of relation {relation.dot_notation} completed in {duration(start_time)}.')
                else:
                    executable.target_adapter.create_database_if_not_exists(
                        relation.database)
                    executable.target_adapter.create_schema_if_not_exists(
                        relation.database,
                        relation.schema)
                    if relation.is_view:
                        logger.info(
                            f'Retrieving DDL statement for view {relation.dot_notation} in source...')
                        relation.population_size = "N/A"
                        relation.sample_size = "N/A"
                        try:
                            relation.view_ddl = executable.source_adapter.scalar_query(
                                relation.compiled_query)
                        except Exception as exc:
                            raise SystemError(
                                f'Failed to extract DDL statement: {relation.compiled_query}') from exc
                        logger.info('Successfully extracted DDL statement for view '
                                    f'{executable.target_adapter.quoted_dot_notation(relation)}')
                    else:
                        logger.info(
                            f'Retrieving records from source {relation.dot_notation}...')
                        try:
                            executable.source_adapter.create_table(
                                query=relation.compiled_query,
                                name=relation.name,
                                schema=unique_schema_name,
                                database=relation.temp_database,
                            )
                            fetch_query = f"SELECT * FROM {relation.temp_database}.{unique_schema_name}.{relation.name}"
                            relation.data = executable.source_adapter.check_count_and_query(
                                fetch_query, relation.sampling.max_allowed_rows, relation.unsampled)
                        except Exception as exc:
                            raise SystemError(
                                f'Failed execution of extraction sql statement: {relation.compiled_query} {exc}') \
                                from exc

                        relation.sample_size = len(relation.data)
                        logger.info(
                            f'{relation.sample_size} records retrieved for relation {relation.dot_notation}.')

                    logger.info(f'Inserting relation {executable.target_adapter.quoted_dot_notation(relation)}'
                                ' into target...')
                    try:
                        executable.target_adapter.create_and_load_relation(
                            relation)
                    except Exception as exc:
                        raise SystemError('Failed to load relation '
                                          f'{executable.target_adapter.quoted_dot_notation(relation)} '
                                          f' into target: {exc}') from exc

                    logger.info('Done replication of relation '
                                f'{executable.target_adapter.quoted_dot_notation(relation)} '
                                f' in {duration(start_time)}.')
                    relation.target_loaded = True
                relation.source_extracted = True
                logger.info(
                    f'population:{relation.population_size}, sample:{relation.sample_size}')
                if self.barf:
                    with open(os.path.join(self.barf_output, f'{relation.dot_notation}.sql'), 'w') as barf_file:  # noqa pylint: disable=unspecified-encoding
                        barf_file.write(relation.compiled_query)

            drop_schemas = set()
            for relation in executable.graph.nodes:
                try:
                    if unique_schema_name not in drop_schemas:
                        executable.source_adapter.drop_schema(
                            unique_schema_name, relation.temp_database)
                        drop_schemas.add(unique_schema_name)
                    del relation.data
                except AttributeError:
                    logger.warning("Failed to purge data of the %s relation", relation)

            gc.collect()
        except Exception as exc:
            logger.error(f'failed with error of type {type(exc)}: {str(exc)}')
            raise exc
