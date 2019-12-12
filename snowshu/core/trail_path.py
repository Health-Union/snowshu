from snowshu.core.utils import lookup_relation, at_least_one_full_pattern_match, single_full_pattern_match
import networkx
import queue
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from copy import deepcopy
from typing import Optional,Any,TextIO,List
from snowshu.core.credentials import Credentials
from pathlib import Path
import os
import yaml
from typing import Union
import snowshu.source_adapters as adapters
from snowshu.logger import Logger
from snowshu.core.relation import Relation
logger=Logger().logger

class TrailPath:

    source_adapter:adapters.BaseSourceAdapter
    dags:List[Relation]

    def __init__(self):
        self._credentials=dict()

    def load_config(self,config:Union[Path,str,TextIO]):
        """ does all the initial work to make the resulting TrailPath object usable."""
        config=self._load_from_file_or_path(config)
        self._load_credentials(self._get_config_value(config,
                                                      "credpath",
                                                      "SNOWSHU_CREDENTIALS_FILEPATH"),
                                                      *[self._get_config_value(config[section], "profile") for section in ('source','target','storage',)])
        self.source_configs=self._get_config_value(config,'source')
        
        self.THREADS=int(self._get_config_value(config,"threads"))
        self._load_adapters()
        self._set_connections()


    def _build_graph(self)->networkx.DiGraph:
        """Builds a directed graph per trail path config"""
        included_relations=set()
        graph=networkx.DiGraph()
        DEFAULT_LOOKUPS=self._get_config_value(self.source_configs,"default_sampling")
        SPECIFIED_LOOKUPS=self._get_config_value(self.source_configs,"specified_relations")

        approved_default_patterns = [dict(database=d['name'], 
                                  schema=s['name'], 
                                  name=r) for d in DEFAULT_LOOKUPS['databases'] \
                                          for s in d['schemas'] \
                                          for r in s['relations']]

        approved_specified_patterns = [dict(database=r['database'],
                                    schema=r['schema'],
                                    name=r['relation']) for r in SPECIFIED_LOOKUPS]

        approved_patterns = approved_default_patterns + approved_specified_patterns
        [included_relations.add(filtered_relation) for filtered_relation in \
            set(filter(lambda rel : at_least_one_full_pattern_match(rel,approved_patterns), self.full_catalog))]

        dependencies=[r['bidirectional'] + r['directional'] for r in SPECIFIED_LOOKUPS][0]
        
        ## add dependency relations
        for dependency in dependencies:
            dep_relation=lookup_relation(dependency,self.full_catalog)
            if dep_relation is None:
                raise ValueError(f"relation \
{dependency['database']}.{dependency['schema']}.{dependency['relation']} \
specified as a dependency but it does not exist.")
            included_relations.add(dep_relation)
        
        ## build graph and add edges
        graph.add_nodes_from(included_relations)
        for relation in SPECIFIED_LOOKUPS:
            relation["name"]=relation["relation"] # hack for the pattern match lookup
            edges=list()
            for direction in ('bidirectional','directional',):
                edges+=[dict(direction=direction,**val) for val in relation[direction]]
            for edge in edges:
                downstream_relations=set(filter(lambda x :single_full_pattern_match(x,relation), included_relations))
                logger.debug(f'upstream relations : {downstream_relations}')
                upstream_relation = lookup_relation(edge,included_relations)
                logger.debug(f'downstream relation: {upstream_relation}.')

                for rel in downstream_relations:
                    graph.add_edge( upstream_relation,
                                    rel,
                                    direction='bidirectional', 
                                    upstream_attribute=edge['remote_attribute'],
                                    downstream_attribute=edge['local_attribute'])
            if not graph.is_directed():
                raise ValueError('The graph created by the specified trail path is not directed (circular reference detected).')
        return graph

    def _build_dags_from_graph(self,graph:networkx.DiGraph)->tuple:
        """builds independent dags and returns the collection of them"""
        ## get isolates first
        isodags=[i for i in networkx.isolates(graph)]        
        logger.info(f'created {len(isodags)} isolate dags.')
        ## assemble undirected graphs 
        ugraph=graph.to_undirected()
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
        
        dags=[networkx.Graph() for _ in range(len(isodags))]
        [g.add_node(n) for g,n in zip(dags,isodags)]

        [dags.append(networkx.subgraph(graph,collection)) for collection in node_collections]
        
        return tuple(dags)
        
    def load_dags(self)->None:
        self.dags=self._build_dags_from_graph(
                        self._build_graph())

    def _load_full_catalog(self)->None:
        full_catalog=list()
        databases = queue.Queue()
        [databases.put(db) for db in self.source_adapter.get_all_databases()]
        
        def accumulate_relations(db,accumulator):
            accumulator+=self.source_adapter.get_relations_from_database(db)
        
        with ThreadPoolExecutor(max_workers=self.THREADS) as executor:
            executor.submit(accumulate_relations,databases,full_catalog)

        self.full_catalog=tuple(full_catalog)

    def _load_adapters(self):
        self._fetch_source_adapter(self._get_config_value(self._credentials['source'],'adapter',parent_name="source"))

    def _set_connections(self):
        creds=deepcopy(self._credentials['source'])
        creds.pop('name')
        creds.pop('adapter')
        self.source_adapter.credentials=Credentials(**creds)

    def _load_credentials(self,credentials_path:str, 
                               source_profile:str, 
                               target_profile:str,
                               storage_profile:str)->None:
        logger.info('loading credentials for adapters...')
        all_creds=self._load_from_file_or_path(credentials_path)
        requested_profiles=dict(source=source_profile,target=target_profile,storage=storage_profile)

        for section in ('source','target','storage',):
            sections=f"{section}s" #pluralize ;)
            logger.debug(f'loading {sections} profile {requested_profiles[section]}...')
            profiles=self._get_config_value(all_creds,sections) 
            for profile in profiles:
                if profile['name'] == requested_profiles[section]:
                    self._credentials[section]=profile
            if section not in self._credentials.keys():
                message=f"{section} profile {requested_profiles[section]} not found in provided credentials."
                logger.error(message)
                raise AttributeError(message)    

    def _fetch_source_adapter(self,adapter_name:str):
        logger.debug('loading source adapter...')
        try:
            classnamed= ''.join([part.capitalize() for part in adapter_name.split('_')] + ['Adapter'])
            self.source_adapter=adapters.__dict__[f"{classnamed}"]()
            logger.debug(f'source adapter set to {classnamed}.')
        except KeyError as e:
            logger.error(f"failed to load config; {adapter_name} is not a valid adapter.{e}")            
            raise e
        
    def _load_from_file_or_path(self,loadable:Union[Path,str,TextIO])->dict:
        try:
            with open(loadable) as f:
                logger.info(f'loading from file {f.name}')
                loaded=yaml.safe_load(f)
        except TypeError:
            logger.info('loading from file-like object...')
            loaded=yaml.safe_load(loadable)        
        logger.info('Done loading.')
        return loaded

    def _get_config_value(self,
                          parent:dict,
                          key:str,
                          envar:Optional[str]=None,
                          parent_name:Optional[str]=None)->Any:   
        try:
            return parent[key]
        except KeyError as e:
            if envar is not None and os.getenv(envar) is not None:
                return os.getenv(envar)
            else:
                message = f'Config issue: missing required attribute \
{key+" from object "+parent_name if parent_name is not None else key}.'
            logger.error(message)
            raise e
            


