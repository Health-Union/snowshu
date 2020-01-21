import networkx 
from snowshu.exceptions import InvalidRelationshipException
from snowshu.core.models.relation import Relation
from snowshu.core.models import materializations as mz 
from snowshu.core.configuration_parser import Configuration
from typing import Tuple,Set,List
from snowshu.logger import Logger
from snowshu.core.utils import get_config_value
from snowshu.core.models.relation import at_least_one_full_pattern_match,\
lookup_relations,\
lookup_single_relation,\
single_full_pattern_match

logger=Logger().logger

class SnowShuGraph:

    def __init__(self):
        self.dag:tuple=None
        self.graph:networkx.Graph=None

    def build_graph(self, configs:Configuration, full_catalog:Tuple[Relation])->networkx.DiGraph:
        """Builds a directed graph per replica config"""
        logger.debug('Building graphs from config...')
        included_relations=self._filter_relations(full_catalog, self._build_sum_patterns_from_configs(configs))

        ## build graph and add edges
        graph=networkx.DiGraph()
        graph.add_nodes_from(included_relations)
        self.graph=self._apply_specifications(configs,graph, full_catalog)
        ## set default sampling at relation level
        [self._set_sampling_method(relation,configs) for relation in self.graph.nodes]
        logger.info(f'Identified a total of {len(self.graph)} relations to sample based on the specified configurations.')

        if not self.graph.is_directed():
            raise ValueError('The graph created by the specified trail path is not directed (circular reference detected).')

    def _apply_specifications(self,configs:Configuration, graph:networkx.DiGraph, available_nodes:Set[Relation])->networkx.DiGraph:
        """ takes a configuration file, a graph and a collection of available nodes, applies configs as edges and returns the graph."""
        for relation in configs.specified_relations:
            relation_dict=dict(name=relation.relation_pattern,database=relation.database_pattern,schema=relation.schema_pattern)
            if relation.unsampled:
                unsampled_relations=set(filter(lambda x :single_full_pattern_match(x,relation_dict), available_nodes))
                for rel in unsampled_relations:
                    rel.unsampled=True
                    graph.add_node(rel)
                continue
            
            edges=list()
            for direction in ('bidirectional','directional',):
                edges+=[dict(   direction=direction,
                                database=val.database_pattern,
                                schema=val.schema_pattern,
                                relation=val.relation_pattern,
                                remote_attribute=val.remote_attribute,
                                local_attribute=val.local_attribute) for val in relation.relationships.__dict__[direction]]
           
            for edge in edges:
                downstream_relations=set(filter(lambda x :single_full_pattern_match(x,relation_dict), available_nodes))
                for rel in downstream_relations:
                    ## populate any string wildcard upstreams
                    for attr in ('database','schema',):
                        edge[attr]=edge[attr] if edge[attr] is not None else getattr(rel,attr)
                    upstream_relation = lookup_single_relation(edge,available_nodes)
                    if upstream_relation is None:
                        raise ValueError(f'It looks like the wildcard relation {edge["database"]}.{edge["schema"]}.{edge["relation"]} was specified as a dependency, but it does not exist.')
                    if upstream_relation.is_view:
                        raise InvalidRelationshipException(f'Relation {upstream_relation.quoted_dot_notation} is a view, but has been specified as an upstream dependency for relation {relation.quoted_dot_notation}. View dependencies are not allowed by SnowShu.')
                    if upstream_relation == rel:
                        continue
                    graph.add_edge( upstream_relation,
                                    rel,
                                    direction=edge['direction'], 
                                    remote_attribute=edge['remote_attribute'],
                                    local_attribute=edge['local_attribute'])
        if not graph.is_directed():
            raise ValueError('The graph created by the specified trail path is not directed (circular reference detected).')
        return graph

    def get_graphs(self)->tuple:
        """builds independent graphs and returns the collection of them"""
        if not isinstance(self.graph,networkx.Graph):
            raise ValueError('Graph must be built before SnowShuGraph can get graphs from it.')
        ## get isolates first
        isodags=[i for i in networkx.isolates(self.graph)]        
        logger.debug(f'created {len(isodags)} isolate dags.')

        ## assemble undirected graphs 
        ugraph=self.graph.to_undirected()
        ugraph.remove_nodes_from(isodags)
        node_collections=list()
        for node in ugraph:
            if len(node_collections) < 1:
                node_collections.append(tuple(networkx.shortest_path(ugraph,node).keys()))
            else:
                for collection in node_collections:
                    if node in collection:
                        collection = collection + tuple(n for n in networkx.shortest_path(ugraph,node).keys() if n not in collection)
                        break
                    else:
                        node_collections.append(tuple(networkx.shortest_path(ugraph,node).keys()))

        dags=[networkx.DiGraph() for _ in range(len(isodags))]
        [g.add_node(n) for g,n in zip(dags,isodags)]
        [dags.append(networkx.subgraph(self.graph,collection)) for collection in node_collections]
        
        ## set the views flag
        for dag in dags:
            dag.contains_views=False
            for relation in dag.nodes:
                dag.contains_views=any((dag.contains_views,relation.is_view,))


        return tuple(dags)
      
    def _build_sum_patterns_from_configs(self,config:Configuration)->List[dict]:
        """ creates pattern dictionaries to filter with to build the total filtered catalog."""
        logger.debug('building sum patterns for configs...')
        approved_default_patterns = [dict(database=d.database_pattern, 
                                  schema=s.schema_pattern, 
                                  name=r.relation_pattern) for d in config.default_sampling.databases \
                                          for s in d.schemas \
                                          for r in s.relations]

        approved_specified_patterns = [dict(database=r.database_pattern,
                                    schema=r.schema_pattern,
                                    name=r.relation_pattern) for r in config.specified_relations]

        all_patterns=approved_default_patterns + approved_specified_patterns 
        logger.debug(f'All config primary primary: {all_patterns}')
        return all_patterns

    def _filter_relations(self, full_catalog:iter, patterns:dict)->Set[Relation]:
        """ applies patterns to the full catalog to build the filtered relation set."""

        return set([filtered_relation for filtered_relation in \
            set(filter(lambda rel : at_least_one_full_pattern_match(rel,patterns), full_catalog))])
    
    def _set_sampling_method(self,relation:Relation,configs:Configuration)->Relation:
        """ for now sets all to default."""
        relation.sample_method=configs.default_sample_method
        return relation
