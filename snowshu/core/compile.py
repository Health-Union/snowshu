from snowshu.adapters.source_adapters.base_source_adapter import BaseSourceAdapter
from snowshu.core.models import Relation
from snowshu.exceptions import InvalidRelationshipException
import networkx
from typing import Type
from snowshu.logger import Logger
logger = Logger().logger


class RuntimeSourceCompiler:

    @staticmethod
    def compile_queries_for_relation(relation: Relation,
                                     dag: networkx.Graph,
                                     source_adapter: Type[BaseSourceAdapter],
                                     analyze: bool) -> Relation:
        """generates and populates the compiled sql for each relation in a
        dag."""

        query = str()
        if relation.is_view:
            relation.core_query, relation.compiled_query = [
                source_adapter.view_creation_statement(relation) for _ in range(2)]
            return relation
        if relation.unsampled:
            query = source_adapter.unsampled_statement(relation)
        else:
            do_not_sample = False
            predicates = list()
            for parent in [p for p in dag.predecessors(relation)]:
                if parent.is_view:
                    raise InvalidRelationshipException(
                        f'Relation {relation.quoted_dot_notation} \
depends on relation {parent.quoted_dot_notation}, which is a view.\
View dependencies are not supported in SnowShu.')

                for edge in dag.edges((parent, relation,), True):
                    edge_data = edge[2]
                    if edge_data['direction'] == 'bidirectional':
                        do_not_sample = True
                    predicates.append(
                        source_adapter.predicate_constraint_statement(
                            parent,
                            analyze,
                            edge_data['local_attribute'],
                            edge_data['remote_attribute']))

            query = source_adapter.sample_statement_from_relation(
                relation, (None if predicates else relation.sample_method))
            if predicates:
                query += " WHERE " + ' AND '.join(predicates)
                query = source_adapter.directionally_wrap_statement(
                    query, (None if do_not_sample else relation.sample_method))

        relation.core_query = query

        if analyze:
            query = source_adapter.analyze_wrap_statement(query, relation)
        relation.compiled_query = query
        return relation
