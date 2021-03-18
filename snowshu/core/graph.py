from typing import List, Set

import networkx

from snowshu.core.configuration_parser import Configuration
from snowshu.core.models.relation import (Relation,
                                          at_least_one_full_pattern_match,
                                          lookup_single_relation,
                                          single_full_pattern_match)
from snowshu.exceptions import InvalidRelationshipException
from snowshu.logger import Logger

logger = Logger().logger


class SnowShuGraph:

    def __init__(self):
        self.dag: tuple = None
        self.graph: networkx.Graph = None

    def build_graph(self, configs: Configuration) -> networkx.DiGraph:
        """Builds a directed graph per replica config.

        Args:
            configs: :class:`Configuration <snowshu.core.configuration_parser.Configuration>` object.

        Returns:
            a directed graph of :class:`Relations <snowshu.core.models.relation.Relation>` with dependencies applied.
        """
        logger.debug('Building graphs from config...')

        full_catalog = configs.source_profile.adapter.build_catalog(
            patterns=self._build_sum_patterns_from_configs(configs),
            thread_workers=configs.threads)
        # set defaults for all relations in the catalog
        for relation in full_catalog:
            self._set_globals_for_node(relation, configs)
            self._set_overriding_params_for_node(relation, configs)

        included_relations = self._filter_relations(
            full_catalog, self._build_sum_patterns_from_configs(configs))

        # build graph and add edges
        graph = networkx.DiGraph()
        graph.add_nodes_from(included_relations)
        self.graph = self._apply_specifications(configs, graph, full_catalog)

        logger.info(
            f'Identified a total of {len(self.graph)} relations to sample based on the specified configurations.')

        if not networkx.algorithms.is_directed_acyclic_graph(self.graph):
            raise ValueError(
                'The graph created by the specified trail path is not directed (circular reference detected).')

    @staticmethod
    def _set_overriding_params_for_node(relation: Relation,
                                        configs: Configuration) -> Relation:
        """Finds and applies specific params from config.

        If multiple conflicting specific params are found they will be applied in descending order from
        the originating replica file.

        Args:
            relation: A :class:`Relation <snowshu.core.models.relation.Relation>` to be tested for specific configs.
            configs: :class:`Configuration <snowshu.core.configuration_parser.Configuration>` object to search for
                matches and specified params.
        Returns:
            The :class:`Relation <snowshu.core.models.relation.Relation>` with all updated params applied.
        """
        for pattern in configs.specified_relations:
            if single_full_pattern_match(relation,
                                         pattern):
                for attr in ('unsampled', 'include_outliers',):
                    pattern_val = getattr(pattern, attr, None)
                    relation.__dict__[
                        attr] = pattern_val if pattern_val is not None else relation.__dict__[attr]

                if getattr(pattern, 'sampling', None) is not None:
                    relation.sampling = pattern.sampling
        return relation

    @staticmethod   # noqa mccabe: disable=MC0001
    def _apply_specifications(
            configs: Configuration,
            graph: networkx.DiGraph,
            available_nodes: Set[Relation]) -> networkx.DiGraph:
        """takes a configuration file, a graph and a collection of available
        nodes, applies configs as edges and returns the graph."""
        for relation in configs.specified_relations:
            relation_dict = dict(
                name=relation.relation_pattern,
                database=relation.database_pattern,
                schema=relation.schema_pattern)
            if relation.unsampled:
                unsampled_relations = set(
                    filter(
                        lambda x: single_full_pattern_match(
                            x,
                            relation_dict),     # noqa pylint: disable=cell-var-from-loop
                        available_nodes))
                for rel in unsampled_relations:
                    rel.unsampled = True
                    graph.add_node(rel)
                continue

            edges = list()
            for direction in ('bidirectional', 'directional',):
                edges += [
                    dict(
                        direction=direction,
                        database=val.database_pattern,
                        schema=val.schema_pattern,
                        relation=val.relation_pattern,
                        remote_attribute=val.remote_attribute,
                        local_attribute=val.local_attribute) for val in relation.relationships.__dict__[direction]]

            for edge in edges:
                downstream_relations = set(
                    filter(
                        lambda x: single_full_pattern_match(
                            x,
                            relation_dict),     # noqa  pylint: disable=cell-var-from-loop
                        available_nodes))
                for rel in downstream_relations:
                    # populate any string wildcard upstreams
                    for attr in ('database', 'schema',):
                        edge[attr] = edge[attr] if edge[attr] is not None else getattr(
                            rel, attr)
                    upstream_relation = lookup_single_relation(
                        edge, available_nodes)
                    if upstream_relation is None:
                        raise ValueError(
                            f'It looks like the wildcard relation '
                            f'{edge["database"]}.{edge["schema"]}.{edge["relation"]} '
                            f'was specified as a dependency, but it does not exist.')
                    if upstream_relation.is_view:
                        raise InvalidRelationshipException(
                            f'Relation {upstream_relation.quoted_dot_notation} is a view, '
                            f'but has been specified as an upstream dependency for '
                            f'relation {relation.quoted_dot_notation}. '
                            f'View dependencies are not allowed by SnowShu.')
                    if upstream_relation == rel:
                        continue
                    graph.add_edge(upstream_relation,
                                   rel,
                                   direction=edge['direction'],
                                   remote_attribute=edge['remote_attribute'],
                                   local_attribute=edge['local_attribute'])
        return graph

    def get_graphs(self) -> tuple:
        """builds independent graphs and returns the collection of them."""
        if not isinstance(self.graph, networkx.Graph):
            raise ValueError(
                'Graph must be built before SnowShuGraph can get graphs from it.')
        # get isolates first
        isodags = list(networkx.isolates(self.graph))
        logger.debug(f'created {len(isodags)} isolate dags.')
        # assemble undirected graphs
        ugraph = self.graph.to_undirected()
        ugraph.remove_nodes_from(isodags)
        node_collections = self._split_dag_for_parallel(ugraph)

        dags = [networkx.DiGraph() for _ in range(len(isodags))]
        for graph, node in zip(dags, isodags):
            graph.add_node(node)

        for collection in node_collections:
            dags.append(networkx.subgraph(self.graph, collection))

        # set the views flag
        for dag in dags:
            dag.contains_views = False
            for relation in dag.nodes:
                dag.contains_views = any(
                    (dag.contains_views, relation.is_view,))

        return tuple(dags)

    @staticmethod
    def _split_dag_for_parallel(dag: networkx.DiGraph) -> list:
        ugraph = dag.to_undirected()
        all_paths = set(frozenset(networkx.shortest_path(ugraph, node).keys())
                        for node in ugraph)
        return list(tuple(node) for node in all_paths)

    @staticmethod
    def _build_sum_patterns_from_configs(config: Configuration) -> List[dict]:
        """creates pattern dictionaries to filter with to build the total
        filtered catalog.

        Args:
            config: :class:`Configuration <snowshu.core.configuration_parser.Configuration>` object.
        """
        logger.debug('building sum patterns for configs...')
        approved_default_patterns = [dict(database=d.database_pattern,
                                          schema=s.schema_pattern,
                                          name=r.relation_pattern) for d in config.general_relations.databases
                                     for s in d.schemas
                                     for r in s.relations]

        approved_specified_patterns = [
            dict(
                database=r.database_pattern,
                schema=r.schema_pattern,
                name=r.relation_pattern) for r in config.specified_relations]

        approved_second_level_specified_patterns = [
            dict(
                database=lower_level.database_pattern if lower_level.database_pattern else upper_level.database_pattern,
                schema=lower_level.schema_pattern if lower_level.schema_pattern else upper_level.schema_pattern,
                name=lower_level.relation_pattern
            ) for upper_level in config.specified_relations
            for lower_level in upper_level.relationships.bidirectional + upper_level.relationships.directional
        ]

        all_patterns = approved_default_patterns + \
            approved_specified_patterns + approved_second_level_specified_patterns
        logger.debug(f'All config primary patterns: {all_patterns}')
        return all_patterns

    @staticmethod
    def _filter_relations(full_catalog: iter,
                          patterns: dict) -> Set[Relation]:
        """applies patterns to the full catalog to build the filtered relation
        set."""

        return set(filter(lambda rel: at_least_one_full_pattern_match(rel, patterns),
                          full_catalog))

    @staticmethod
    def _set_globals_for_node(relation: Relation, configs: Configuration) -> Relation:
        """Sets the initial (default) node values from the config

        ARGS:
            relation: the :class:`Relation <snowshu.core.models.relation>` to set values of.
            configs: the :class:`Configuration <snowshu.core.configuration_parser.Configuration`
                object to derive default values from.

        Returns:
            The updated :class:`Relation <snowshu.core.models.relation>`
        """
        relation.sampling = configs.sampling
        relation.include_outliers = configs.include_outliers
        relation.max_number_of_outliers = configs.max_number_of_outliers
        return relation
