from snowshu.adapters.source_adapters.sample_methods import SampleMethod
from snowshu.adapters.source_adapters.base_source_adapter import BaseSourceAdapter
from snowshu.core.models import Relation
import networkx
from typing import List
from snowshu.logger import Logger
logger=Logger().logger


class BaseCompiler:
    
    def __init__(self,
                 dags:List[networkx.Graph],
                 adapter:BaseSourceAdapter,
                 sample_method:SampleMethod,
                 analyze:bool=False):
        self.dags=dags
        self.adapter=adapter
        self.sample_method=sample_method
        self.analyze=analyze        

    def compile(self):
        for dag in self.dags:
            self._compile_dag(dag)
        return self.dags

    def _compile_dag(self,dag:List[networkx.DiGraph]):
        """ For each relation in the dag:
                - create a base sample statement
                - append predicates as select-unions or subquery (based on analyze value)
                - wrap with counts if analyze
        """
        for relation in networkx.algorithms.dag.topological_sort(dag): #topo puts them in order of dag execution
            query=str()
            if relation.unsampled:
                query=self.adapter.unsampled_statement(relation)
            else:    
                do_not_sample=False
                predicates=list()
                for parent in [p for p in dag.predecessors(relation)]:
                    for edge in dag.edges((parent,relation,),True):
                        edge_data=edge[2]
                        if edge_data['direction']=='bidirectional':
                            do_not_sample=True
                        predicates.append(self.adapter.predicate_constraint_statement(parent,
                                                                                      self.analyze,
                                                                                      edge_data['local_attribute'],
                                                                                      edge_data['remote_attribute']))
                

                query=self.adapter.sample_statement_from_relation(relation, (None if predicates else self.sample_method))
                if predicates:
                    query+= " WHERE " + ' AND '.join(predicates)
                    query=self.adapter.directionally_wrap_statement(query,(None if do_not_sample else self.sample_method))
                
            relation.core_query=query
            
            if self.analyze:
                query=self.adapter.analyze_wrap_statement(query,relation)
            relation.compiled_query=query

        
    def construct_compiled_query(   self,
                                    relation:Relation,
                                    dag:networkx.DiGraph,
                                    adapter:BaseSourceAdapter,
                                    analyze:bool)->str:
        query=str()
        if relation.unsampled:
            query=self.adapter.unsampled_statement(relation)
        else:    
            do_not_sample=False
            predicates=list()
            for parent in [p for p in dag.predecessors(relation)]:
                for edge in dag.edges((parent,relation,),True):
                    edge_data=edge[2]
                    if edge_data['direction']=='bidirectional':
                        do_not_sample=True
                    predicates.append(self.adapter.predicate_constraint_statement(parent,
                                                                                  self.analyze,
                                                                                  edge_data['local_attribute'],
                                                                                  edge_data['remote_attribute']))
            

            query=adapter.sample_statement_from_relation(relation, (None if predicates else relation.sample_method))
            if predicates:
                query+= " WHERE " + ' AND '.join(predicates)
                query=self.adapter.directionally_wrap_statement(query,(None if do_not_sample else relation.sample_method))
            
        relation.core_query=query
        
        if analyze:
            query=adapter.analyze_wrap_statement(query,relation)
        return query
