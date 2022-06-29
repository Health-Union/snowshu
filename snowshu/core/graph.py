import os.path
from datetime import datetime
from itertools import chain
from typing import List, Set, Tuple, Optional, Union
import logging

import matplotlib.pyplot as plt
import networkx

from snowshu.core.configuration_parser import Configuration
from snowshu.core.graph_set_runner import GraphSetRunner
from snowshu.core.models.relation import (Relation,
                                          single_full_pattern_match)
from snowshu.exceptions import InvalidRelationshipException

logger = logging.getLogger(__name__)


class SnowShuGraph:
    """ Wrapper class for the networkx.Graph that represents the
        configuration relation dependencies

        Attributes:
            dag (Optional[tuple]) - unused
            graph (Optional[networkx.Graph]) - Graph representation of a configuration
    """

    def __init__(self):
        self.dag: Optional[tuple] = None
        self.graph: Optional[networkx.Graph] = None

    @staticmethod
    def catalog_difference(source_graph: Union["SnowShuGraph", networkx.Graph],
                           target_catalog: Union[tuple, list, set]) -> networkx.Graph:
        """ Finds a difference between source_graph nodes and target_catalog nodes and deletes other nodes
        (common, not present in difference) from source_graph.

            Args:
                source_graph (Union[SnowShuGraph, networkx.Graph]): source Graph or SnowShuGraph object
                    which is built in current run from replica.yml file.
                target_catalog (Union[tuple, list, set]): object which is generated by getting target
                    catalog from existing replica image.

            Returns:
                The :class:`Graph <networkx.Graph>` which is source graph with removed nodes which are
                    common with target catalog nodes.

        """
        if isinstance(source_graph, SnowShuGraph):
            source_graph = source_graph.graph

        non_isolated_relations = {}
        for relation, relation_base in source_graph.edges:
            if non_isolated_relations.get(relation_base):
                non_isolated_relations[relation_base].append(relation)
            else:
                non_isolated_relations[relation_base] = [relation]

        non_isolated_relations = [(relation_base, *relations)
                                  for relation_base, relations in non_isolated_relations.items()]
        isolated_relations = list(networkx.isolates(source_graph))

        non_isolated_relations_difference = chain.from_iterable([entries for entries in non_isolated_relations
                                                                 if not all(entry in target_catalog
                                                                            for entry in entries)])

        isolated_relations_difference = set(isolated_relations).difference(target_catalog)

        difference_catalog = isolated_relations_difference.union(non_isolated_relations_difference)
        nodes_to_delete = [node for node in source_graph if node not in difference_catalog]
        source_graph.remove_nodes_from(nodes_to_delete)
        return source_graph

    @staticmethod
    def _build_graph_cycles_output(graph_cycles: list) -> Tuple[str, str]:
        """ Builds simple cycles output according to the list of graph_cycles.

            Args:
                graph_cycles: list of simple cycles in the directed graph built per replica config.

            Returns:
                The :rtype: Tuple[str, str]: `message`, `filename`,
                the `message` represents the list of the simple cycles in the graph,
                the `filename` corresponds to the name of .png and .graphml files where cycles image is saved.
        """

        cycle_graph = networkx.DiGraph()
        message = ""
        nodes = []

        # create a message with list of nodes for each simple cycle, add nodes and edges to the graph
        for cycle in graph_cycles:
            for i, node in enumerate(cycle):
                nodes.append(node)
                if i != len(cycle) - 1:
                    cycle_graph.add_edge(node, cycle[i + 1])
                    message = message + '\033[1;34m' + str(node)[17:-1] + '\t\033[1;32m----\t'
                else:
                    cycle_graph.add_edge(node, cycle[0])
                    message = message + '\033[1;34m' + str(node)[17:-1]
            message = message + '\n\t'

        filename = ''

        # create output .png and .graphml files in case of existing output directory
        if os.path.isdir(f'{GraphSetRunner.barf_output}/'):
            # label each node as DataBase.Schema.Table
            nodes = set(nodes)
            label_dict = {}
            for node in cycle_graph.nodes():
                label_dict[node] = str(node)[17:-1].replace('.', '\n')

            # make an image for the existing simple cycles
            created_at = datetime.now()
            plt.figure(figsize=(8, 8))
            plt.margins(0.1)
            plt.title(f'\nGraph of the simple cycles, created at: {created_at.strftime("%Y/%m/%d %H:%M:%S")}')

            networkx.draw(
                cycle_graph, pos=networkx.planar_layout(cycle_graph), labels=label_dict, with_labels=True,
                node_size=1000, node_color='skyblue', font_size=6, font_color='green', width=2,
                horizontalalignment='center', verticalalignment='center', connectionstyle='arc3, rad=0.05')

            filename = f'{GraphSetRunner.barf_output}/graph_cycles_{created_at.strftime("%Y_%m_%d_%H_%M_%S")}'
            plt.savefig(f'{filename}.png', bbox_inches='tight', pad_inches=0, dpi=1000)
            networkx.write_graphml(cycle_graph, f'{filename}.graphml')

        return message, filename

    def build_graph(self, configs: Configuration) -> None:
        """ Builds a directed graph per replica config.

            Args:
                configs: :class:`Configuration <snowshu.core.configuration_parser.Configuration>` object.
        """
        logger.debug('Building graph from config...')

        catalog = configs.source_profile.adapter.build_catalog(
            patterns=self.build_sum_patterns_from_configs(configs),
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
            graph_cycles = list(networkx.simple_cycles(self.graph))
            message, filename = self._build_graph_cycles_output(graph_cycles)

            if filename:
                logger.error(
                    'The dependency graph generated by the given specified relations yields a cyclic graph. \
                    \n\tCyclic dependency found in the following relations:\n\t%s \
                    \n\t\033[1;37mThe network topology diagram has been saved to: \
                    \n\t\033[0;36m%s.png \033[1;37m and \033[0;36m%s.graphml', message, filename, filename)
            else:
                logger.error(
                    'The dependency graph generated by the given specified relations yields a cyclic graph. \
                    \n\tCyclic dependency found in the following relations:\n\t%s', message)

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

    @staticmethod  # noqa mccabe: disable=MC0001
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
                            relation_pattern_dict),  # noqa pylint: disable=cell-var-from-loop
                        available_nodes))
                for uns_rel in unsampled_relations:
                    uns_rel.unsampled = True
                    graph.add_node(uns_rel)
                continue

            # processing for non-unsampled relations
            # create a list of the relationship remote patterns and attributes
            relationship_dicts = []
            for relationship_type in ('bidirectional', 'directional',):
                relationship_dicts += [
                    dict(
                        database=val.database_pattern,
                        schema=val.schema_pattern,
                        name=val.relation_pattern,
                        edge_attributes={
                            "direction": relationship_type,
                            "remote_attribute": val.remote_attribute,
                            "local_attribute": val.local_attribute
                        }
                    ) for val in relation.relationships.__dict__[relationship_type]]

            for val in relation.relationships.polymorphic:
                edge_attr = {
                    "direction": "polymorphic",
                    "remote_attribute": val.remote_attribute,
                    "local_attribute": val.local_attribute,
                }
                if val.local_type_attribute:
                    edge_attr["local_type_attribute"] = val.local_type_attribute
                    edge_attr["local_type_overrides"] = val.local_type_overrides

                rel_dict = {
                    "database": val.database_pattern,
                    "schema": val.schema_pattern,
                    "name": val.relation_pattern,
                    "edge_attributes": edge_attr
                }
                relationship_dicts.append(rel_dict)

            # determine downstream relations from relation patterns
            downstream_relations = set(
                filter(
                    lambda x: single_full_pattern_match(x, relation_pattern_dict), # noqa  pylint: disable=cell-var-from-loop
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
            full_relation_set: Set[Relation]) -> networkx.Graph:
        """ Adds the appropriate edges to the graph for the given relationship """
        # pylint: disable-msg=too-many-locals
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
        view_relations = [r for r in upstream_relations if r.is_view]
        if view_relations:
            raise InvalidRelationshipException(
                f'Relations {view_relations} are views, '
                f'but have been specified as an upstream dependency for '
                f'the relationship {relationship}. '
                f'View dependencies are not allowed by SnowShu.'
            )

        catalog_dict = dict(map(lambda x: (x.dot_notation, dict(map(lambda a: (a.name, a.data_type), x.attributes))),
                                full_relation_set))

        is_valid_graph = True
        for downstream_relation in downstream_set:
            for upstream_relation in upstream_without_downstream:
                # validate whether relation is valid
                attributes = dict(filter(lambda item: item[0] in ['remote_attribute', 'local_attribute'],
                                         relationship['edge_attributes'].items()))
                remote_attribute = attributes.get('remote_attribute')
                local_attribute = attributes.get('local_attribute')
                is_remote_attribute_valid = remote_attribute in catalog_dict.get(upstream_relation.dot_notation, {})
                is_local_attribute_valid = local_attribute in catalog_dict.get(downstream_relation.dot_notation, {})

                if not is_remote_attribute_valid or not is_local_attribute_valid:
                    is_valid_graph = False
                    logger.warning(
                        f'Edge {upstream_relation.dot_notation}({remote_attribute}) -> '
                        f'{downstream_relation.dot_notation}({local_attribute}) '
                        f'was incorrectly defined. Please verify the replica config file.'
                    )

                graph.add_edge(upstream_relation,
                               downstream_relation,
                               **relationship['edge_attributes'])

        if not is_valid_graph:
            raise InvalidRelationshipException(
                'Some of the edge(s) were incorrectly defined. '
                'Please ensure that remote_attribute & local_attribute are correctly defined.'
            )

        return graph

    def get_connected_subgraphs(self) -> tuple:
        """ Generates the set of (weakly) connected components of the object's graph

            Returns:
                tuple of connected subgraphs of the original processing graph
        """

        if not isinstance(self.graph, networkx.Graph):
            raise ValueError('Graph must be built before SnowShuGraph can get graphs from it.')

        dags = [networkx.induced_subgraph(self.graph, bunch)
                for bunch in networkx.weakly_connected_components(self.graph)]

        # set the views flag
        for dag in dags:
            dag.contains_views = False
            for relation in dag.nodes:
                dag.contains_views = any((dag.contains_views, relation.is_view,))

        return tuple(dags)

    @staticmethod
    def build_sum_patterns_from_configs(config: Configuration) -> List[dict]:
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
            for lower_level in
            upper_level.relationships.bidirectional + upper_level.relationships.directional + upper_level.relationships.polymorphic # noqa pep8: E501
        ]

        all_patterns = approved_default_patterns + approved_specified_patterns + approved_second_level_specified_patterns  # noqa pep8: E501

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
