from typing import List, Set

import networkx

from snowshu.core.configuration_parser import Configuration
from snowshu.core.models.relation import (Relation,
                                          single_full_pattern_match)
from snowshu.exceptions import InvalidRelationshipException
from snowshu.logger import Logger

logger = Logger().logger


class SnowShuGraph:
    """ Wrapper class for the networkx.Graph that represents the 
        configuration relation dependencies

        Attributes:
            dag (tuple) - unused
            graph (networkx.Graph) - Graph representation of a configuration
    """

    def __init__(self):
        self.dag: tuple = None
        self.graph: networkx.Graph = None

    def build_graph(self, configs: Configuration) -> None:
        """ Builds a directed graph per replica config.

            Args:
                configs: :class:`Configuration <snowshu.core.configuration_parser.Configuration>` object.
        """
        logger.debug('Building graph from config...')

        catalog = configs.source_profile.adapter.build_catalog(
            patterns=self._build_sum_patterns_from_configs(configs),
            thread_workers=configs.threads)
        # set defaults for all relations in the catalog
        for relation in catalog:
            self._set_globals_for_node(relation, configs)
            self._set_overriding_params_for_node(relation, configs)

        # build graph and add edges
        graph = networkx.DiGraph()
        graph.add_nodes_from(catalog)
        self.graph = self._apply_specifications(configs, graph, catalog)

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
    def _apply_specifications(  # noqa pylint: disable=too-many-locals
            configs: Configuration,
            graph: networkx.DiGraph,
            available_nodes: Set[Relation]) -> networkx.DiGraph:
        """ Takes a configuration file, a graph and a collection of available
            nodes, applies configs as edges and returns the graph.

            When edges are added, they are always out of the remote relation
            and into the local relation. The other details of the relationship
            are included in the edge data.

            Args:
                configs: Configuration to translate into a digraph
                graph: The graph object to apply edges to. Assumed to have most nodes included already
                available_nodes: The set of nodes that are available to be in the graph

            Returns:
                - The final digraph with edges that represents the given configuration
        """
        for relation in configs.specified_relations:
            # create dict for pattern matching of specified relation pattern
            relation_pattern_dict = dict(
                name=relation.relation_pattern,
                database=relation.database_pattern,
                schema=relation.schema_pattern)
            # if the relation is unsampled, set all matching nodes to be unsampled and break back to for loop
            if relation.unsampled:
                unsampled_relations = set(
                    filter(
                        lambda x: single_full_pattern_match(
                            x,
                            relation_pattern_dict),     # noqa pylint: disable=cell-var-from-loop
                        available_nodes))
                for uns_rel in unsampled_relations:
                    uns_rel.unsampled = True
                    graph.add_node(uns_rel)
                continue

            # processing for non-unsampled relations
            # create a list of the relationship remote patterns and attributes
            relationship_dicts = list()
            for relationship_type in ('bidirectional', 'directional',):
                relationship_dicts += [
                    dict(
                        direction=relationship_type,
                        database=val.database_pattern,
                        schema=val.schema_pattern,
                        name=val.relation_pattern,
                        remote_attribute=val.remote_attribute,
                        local_attribute=val.local_attribute
                    ) for val in relation.relationships.__dict__[relationship_type]]

            # determine downstream relations from relation patterns
            downstream_relations = set(
                filter(
                    lambda x: single_full_pattern_match(x, relation_pattern_dict),  # noqa  pylint: disable=cell-var-from-loop
                    available_nodes
                )
            )
            if not downstream_relations:
                raise InvalidRelationshipException(
                    f'Relationship {relation_pattern_dict} was specified, '
                    f'but does not match any relations. '
                    f'Please verify replica configuration.'
                )

            # for each relationship, find up/downstream relations, then create the appropriate edges
            for relationship in relationship_dicts:
                # check for wild cards
                possible_wildcard_attrs = ('database', 'schema',)
                wildcard_attrs = [attr for attr in possible_wildcard_attrs if relationship[attr] is None]

                # if there are wildcard attributes, partition downstream relations
                if wildcard_attrs:
                    wildcard_partitions = {}
                    for down_rel in downstream_relations:
                        # create wildcard key off of (in case there are multiple wildcards)
                        wildcard_key = '|'.join([getattr(down_rel, attr) for attr in wildcard_attrs])
                        val = wildcard_partitions.get(wildcard_key, [])
                        val.append(down_rel)
                        wildcard_partitions[wildcard_key] = val

                    for _, downstream_partition in wildcard_partitions.items():
                        # populate any wildcard patterns with the appropriate values from first element
                        for attr in wildcard_attrs:
                            relationship[attr] = getattr(downstream_partition[0], attr)

                        graph = SnowShuGraph._process_downstream_relation_set(relationship,
                                                                              downstream_partition,
                                                                              graph,
                                                                              available_nodes)
                # no wildcards present in relationship definition
                else:
                    graph = SnowShuGraph._process_downstream_relation_set(relationship,
                                                                          downstream_relations,
                                                                          graph,
                                                                          available_nodes)

        return graph

    @staticmethod
    def _process_downstream_relation_set(
            relationship: dict,
            downstream_set: Set[Relation],
            graph: networkx.DiGraph,
            full_relation_set: Set[Relation]) -> None:
        """ Adds the appropriate edges to the graph for the given relationship """
        # find any of the upstream relations
        upstream_relations = set(
            filter(
                lambda x: single_full_pattern_match(x, relationship),  # noqa pylint: disable=cell-var-from-loop
                full_relation_set
            )
        )
        # determine the set difference for verification
        upstream_without_downstream = upstream_relations.difference(downstream_set)

        # check to make sure an upstream relation was found
        if not upstream_without_downstream:
            raise InvalidRelationshipException(
                f'It looks like the relation '
                f'{relationship["database"]}.{relationship["schema"]}.{relationship["name"]} '
                f'was specified as a dependency, but it does not exist.'
            )
        # check to see if there was an intersection between the upstream and downstream relations
        if len(upstream_without_downstream) != len(upstream_relations):
            logger.warning(
                f'Relationship {relationship} defines at least one downstream '
                f'relation in the upstream relation set. Ignoring the occurrence '
                f'in the upstream set. Please verify replica config file.'
            )
        # check to make sure we aren't trying to generate a many-to-many relationship
        if len(upstream_without_downstream) > 1 and len(downstream_set) > 1:
            raise InvalidRelationshipException(
                f'Relationship {relationship} defines a many-to-many '
                f'relationship between tables in the source location. '
                f'Many-to-many relationship are not allowed by SnowShu '
                f'as they are usually unintended side effects of lenient regex.'
            )
        # check to make sure found upstream relation is not a view
        view_relations = [r.quoted_dot_notation for r in upstream_relations if r.is_view]
        if view_relations:
            raise InvalidRelationshipException(
                f'Relations {view_relations} are views, '
                f'but have been specified as an upstream dependency for '
                f'the relationship {relationship}. '
                f'View dependencies are not allowed by SnowShu.'
            )

        for downstream_relation in downstream_set:
            for upstream_relation in upstream_without_downstream:
                graph.add_edge(upstream_relation,
                               downstream_relation,
                               direction=relationship['direction'],
                               remote_attribute=relationship['remote_attribute'],
                               local_attribute=relationship['local_attribute'])
        return graph

    def get_graphs(self) -> tuple:
        """ Generates the set of (weakly) connected components of the object's graph

            Returns:
                tuple of connected subgraphs of the original processing graph
        """
        # TODO this function is manually finding the connected components of the main graph
        # Can likely be simplified if not entirely replaced by get_weakly_connected_components()

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
            for lower_level in upper_level.relationships.bidirectional + upper_level.relationships.directional + upper_level.relationships.polymorphic  # noqa pep8: E501
        ]

        all_patterns = approved_default_patterns + \
            approved_specified_patterns + approved_second_level_specified_patterns
        logger.debug(f'All config primary patterns: {all_patterns}')
        return all_patterns

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
