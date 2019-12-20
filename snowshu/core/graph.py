import networkx 
from snowshu.core.models.relation import Relation
from typing import Tuple,Set,List
from snowshu.logger import Logger
from snowshu.core.utils import get_config_value,\
at_least_one_full_pattern_match,\
lookup_relation,\
single_full_pattern_match

logger=Logger().logger

class SnowShuGraph:

    def __init__(self):
        self.dag:tuple=None
        self.graph:networkx.Graph=None

    def build_graph(self, configs:dict, full_catalog:Tuple[Relation])->networkx.DiGraph:
        """Builds a directed graph per trail path config"""
        logger.debug('Building graphs from config...')
        included_relations=self._filter_relations(full_catalog, self._build_sum_patterns_from_configs(configs))
        SPECIFIED_RELATIONS=get_config_value(configs,"specified_relations")
        if SPECIFIED_RELATIONS:
            included_relations=included_relations.union(self._get_dependency_relations(full_catalog,SPECIFIED_RELATIONS))
        logger.info(f'Identified a total of {len(included_relations)} relations to sample based on the specified configurations.')

        ## build graph and add edges
        graph=networkx.DiGraph()
        graph.add_nodes_from(included_relations)
        
        for relation in SPECIFIED_RELATIONS:
            relation["name"]=relation["relation"] # hack for the pattern match lookup
            edges=list()
            for direction in ('bidirectional','directional',):
                edges+=[dict(direction=direction,**val) for val in relation['relationships'][direction]]
           
            for edge in edges:
                downstream_relations=set(filter(lambda x :single_full_pattern_match(x,relation), included_relations))
                upstream_relation = lookup_relation(edge,included_relations)
                downstream_relations.discard(upstream_relation)
                for rel in downstream_relations:
                    graph.add_edge( upstream_relation,
                                    rel,
                                    direction='bidirectional', 
                                    remote_attribute=edge['remote_attribute'],
                                    local_attribute=edge['local_attribute'])
        if not graph.is_directed():
            raise ValueError('The graph created by the specified trail path is not directed (circular reference detected).')
        logger.debug(f'built graph with {len(graph)} total nodes.')
        self.graph=graph

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
        return tuple(dags)
      
    def _build_sum_patterns_from_configs(self,configs:dict)->List[dict]:
        """ creates pattern dictionaries to filter with to build the total filtered catalog."""
        logger.debug('building sum patterns for configs...')
        DEFAULT_LOOKUPS=get_config_value(configs,"default_sampling")
        SPECIFIED_LOOKUPS=get_config_value(configs,"specified_relations")
        approved_default_patterns = [dict(database=d['name'], 
                                  schema=s['name'], 
                                  name=r) for d in DEFAULT_LOOKUPS['databases'] \
                                          for s in d['schemas'] \
                                          for r in s['relations']]

        approved_specified_patterns = [dict(database=r['database'],
                                    schema=r['schema'],
                                    name=r['relation']) for r in SPECIFIED_LOOKUPS]

        all_patterns=approved_default_patterns + approved_specified_patterns 
        logger.debug(f'All config primary primary: {all_patterns}')
        return all_patterns

    def _get_dependency_relations(self,full_catalog:iter,specified_configs:dict)->Set[Relation]:
        """ gets relations for each dependency in the specified configs"""
        dependencies=[r for parent in specified_configs\
                      for r in parent['relationships']['bidirectional'] + parent['relationships']['directional']]
        dep_relations=set()   
        ## add dependency relations
        for dependency in dependencies:
            dep_relation=lookup_relation(dependency,full_catalog)
            if dep_relation is None:
                raise ValueError(f"relation \
{dependency['database']}.{dependency['schema']}.{dependency['relation']} \
specified as a dependency but it does not exist.")
            dep_relations.add(dep_relation)
        return dep_relations


    def _filter_relations(self, full_catalog:iter, patterns:dict)->Set[Relation]:
        """ applies patterns to the full catalog to build the filtered relation set."""

        return set([filtered_relation for filtered_relation in \
            set(filter(lambda rel : at_least_one_full_pattern_match(rel,patterns), full_catalog))])
    

