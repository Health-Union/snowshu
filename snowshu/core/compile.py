from typing import Type

import networkx

from snowshu.adapters.source_adapters.base_source_adapter import \
    BaseSourceAdapter
from snowshu.core.models import Relation
from snowshu.logger import Logger

logger = Logger().logger


class RuntimeSourceCompiler:

    # TODO breakout edge logic into edgetype/direction handling functions
    @staticmethod   # noqa mccabe: disable=MC0001
    def compile_queries_for_relation(relation: Relation,  # noqa pylint: disable=too-many-branches
                                     dag: networkx.Graph,
                                     source_adapter: Type[BaseSourceAdapter],
                                     analyze: bool) -> Relation:
        """ Generates the sql statements for the given relation

            Args:
                relation (Relation): the relation to generate the sql for
                dag (Graph): the connected dependency graph that contains the relation
                source_adapter (BaseSourceAdapter): the source adapter for the sql dialect
                analyze (bool): whether to generate sql statements for analyze or actaul sampling

            Returns:
                Relation: the given relation with `compiled_query` populated
        """
        if relation.is_view:
            relation.core_query, relation.compiled_query = [
                source_adapter.view_creation_statement(relation) for _ in range(2)]
            return relation
        if relation.unsampled:
            query = source_adapter.unsampled_statement(relation)
        else:
            do_not_sample = False
            predicates = list()
            unions = list()
            polymorphic_predicates = list()
            for child in dag.successors(relation):
                # parallel edges aren't currently supported
                edge = dag.edges[relation, child]
                if edge['direction'] == 'bidirectional':
                    predicates.append(source_adapter.upstream_constraint_statement(child,
                                                                                   edge['remote_attribute'],
                                                                                   edge['local_attribute']))
                if relation.include_outliers and edge['direction'] == 'polymorphic':
                    logger.warning("Polymorphic relationships currently do not support including outliers. "
                                   "Ignoring include_outliers flag for edge "
                                   f"from {relation.dot_notation} to {child.dot_notation}. ")
                elif relation.include_outliers:
                    unions.append(source_adapter.union_constraint_statement(relation,
                                                                            child,
                                                                            edge['remote_attribute'],
                                                                            edge['local_attribute'],
                                                                            relation.max_number_of_outliers))

            for parent in dag.predecessors(relation):
                edge = dag.edges[parent, relation]
                # if any incoming edge is bidirectional or polymorphic set do_not_sample flag
                # do_not_sample is set since those types are most likely already restricted
                do_not_sample = (edge['direction'] in ('bidirectional', 'polymorphic',) or do_not_sample)
                if edge['direction'] == 'polymorphic':
                    # if the local type attribute is set, the constraint needs to account for it
                    # otherwise we only need the normal predicate constraint
                    if 'local_type_attribute' in edge:
                        local_type_override = edge['local_type_overrides'].get(parent.dot_notation, None)
                        polymorphic_predicates.append(
                            source_adapter.polymorphic_constraint_statement(parent,
                                                                            analyze,
                                                                            edge['local_attribute'],
                                                                            edge['remote_attribute'],
                                                                            edge['local_type_attribute'],
                                                                            local_type_override))
                    else:
                        polymorphic_predicates.append(
                            source_adapter.predicate_constraint_statement(parent,
                                                                          analyze,
                                                                          edge['local_attribute'],
                                                                          edge['remote_attribute']))
                else:
                    predicates.append(source_adapter.predicate_constraint_statement(parent,
                                                                                    analyze,
                                                                                    edge['local_attribute'],
                                                                                    edge['remote_attribute']))
                if relation.include_outliers and edge['direction'] == 'polymorphic':
                    logger.warning("Polymorphic relationships currently do not support including outliers. "
                                   "Ignoring include_outliers flag for edge "
                                   f"from {parent.dot_notation} to {relation.dot_notation}. ")
                elif relation.include_outliers:
                    unions.append(source_adapter.union_constraint_statement(relation,
                                                                            parent,
                                                                            edge['local_attribute'],
                                                                            edge['remote_attribute'],
                                                                            relation.max_number_of_outliers))

            # if polymorphic predicates are set up, then generate the or predicate
            if polymorphic_predicates:
                full_polymorphic_predicate = " OR ".join(polymorphic_predicates)
                predicates.append(f"( {full_polymorphic_predicate} )")

            query = source_adapter.sample_statement_from_relation(
                relation, (None if predicates else relation.sampling.sample_method))
            if predicates:
                query += " WHERE " + ' AND '.join(predicates)
                query = source_adapter.directionally_wrap_statement(
                    query, relation, (None if do_not_sample else relation.sampling.sample_method))
            if unions:
                query += " UNION ".join([''] + unions)

        relation.core_query = query

        if analyze:
            query = source_adapter.analyze_wrap_statement(query, relation)
        relation.compiled_query = query
        return relation
