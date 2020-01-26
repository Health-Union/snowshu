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
            query=source_adapter.unsampled_statement(relation)
        else:    
            do_not_sample=False
            predicates=list()
            unions=list()
            for child in [c for c in dag.successors(relation)]:
                for edge in dag.edges((relation,child),True):
                    edge_data=edge[2]
                    if edge_data['direction']=='bidirectional':
                        predicates.append(source_adapter.upstream_constraint_statement( child,
                                                                                        edge_data['remote_attribute'],
                                                                                        edge_data['local_attribute']))
                    if relation.include_outliers:
                        unions.append(source_adapter.union_constraint_statement(relation,
                                                                                child,
                                                                                edge_data['remote_attribute'],
                                                                                edge_data['local_attribute'],
                                                                                relation.max_number_of_outliers))

            for parent in [p for p in dag.predecessors(relation)]:
                for edge in dag.edges((parent,relation,),True):
                    edge_data=edge[2]
                    do_not_sample=edge_data['direction']=='bidirectional'
                    predicates.append(source_adapter.predicate_constraint_statement(parent,
                                                                                  analyze,
                                                                                  edge_data['local_attribute'],
                                                                                  edge_data['remote_attribute']))
                    if relation.include_outliers:
                        unions.append(source_adapter.union_constraint_statement(  relation,
                                                                                  parent,
                                                                                  edge_data['local_attribute'],
                                                                                  edge_data['remote_attribute'],
                                                                                  relation.max_number_of_outliers))
            
            query=source_adapter.sample_statement_from_relation(relation, (None if predicates else relation.sampling.sample_method))
            if predicates:
                query+= " WHERE " + ' AND '.join(predicates)
                query=source_adapter.directionally_wrap_statement(query,relation,(None if do_not_sample else relation.sampling.sample_method))
            if unions:
                query+= " UNION ".join(['']+unions)
        
        relation.core_query=query
        

        if analyze:
            query = source_adapter.analyze_wrap_statement(query, relation)
        relation.compiled_query = query
        return relation
