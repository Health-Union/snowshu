import time
from copy import deepcopy
from typing import TextIO,List,Type,Union
from pathlib import Path
import networkx

from snowshu.core.replica import Replica
from snowshu.core.graph import SnowShuGraph
from snowshu.core.utils import get_config_value, load_from_file_or_path
from snowshu.core.catalog import Catalog
from snowshu.core.models.credentials import Credentials
import snowshu.adapters.source_adapters as source_adapters
import snowshu.adapters.target_adapters as target_adapters
from snowshu.core.sample_methods import get_sample_method_from_kwargs, SampleMethod
from snowshu.logger import Logger, duration
from snowshu.core.models.relation import Relation
from snowshu.core.graph_set_runner import GraphSetRunner
from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.printable_result import graph_to_result_list,printable_result
logger=Logger().logger

class ReplicaFactory:
    
    source_adapter:source_adapters.BaseSourceAdapter
    target_adapter:target_adapters.BaseTargetAdapter
    graphs:List[networkx.Graph]

    def __init__(self):
        self._credentials=dict()

    def run(self,tag:str)->None:
        self.ANALYZE=False
        return self._execute()

    def analyze(self)->None:
        self.ANALYZE=True
        return self._execute()
     

    def _execute(self)->None:
        self.graphs=self._build_uncompiled_graphs()
        if len(self.graphs) < 1:
            return "No relations found per provided replica configuration, exiting."

        self.target_adapter.initialize_replica()
        runner=GraphSetRunner()
        runner.execute_graph_set(   self.graphs,
                                    self.source_adapter,
                                    self.target_adapter,
                                    threads=self.config.threads,
                                    analyze=self.ANALYZE)
    
        self.target_adapter.finalize_replica()

        return printable_result(graph_to_result_list(self.graphs,
                                                     self.config.default_sample_method),
                                self.ANALYZE)

    def load_config(self,config:Union[Path,str,TextIO]):
        """ does all the initial work to make the resulting ReplicaFactory object usable."""
        logger.info('Loading credentials...')
        start_timer=time.time()
        self.config=ConfigurationParser.from_file_or_path(config)
        
        self._load_credentials( self.config.credpath,
                                self.config.source_profile,
                                self.config.storage_profile)
        

        self._load_adapters()
        self._set_connections()
        logger.info(f'Credentials loaded in {duration(start_timer)}.')



    def _load_full_catalog(self)->None:
        logger.info('Assessing full catalog...')
        start_timer=time.time()
        catalog=Catalog(self.source_adapter,self.config.threads)
        catalog.load_full_catalog()
        self.full_catalog=catalog.catalog
        logger.info(f'Done assessing catalog. Found a total of {len(self.full_catalog)} relations from the source in {duration(start_timer)}.')

    def _load_adapters(self):
        self._fetch_adapter('source',
                            get_config_value(self._credentials['source'],
                                            'adapter',
                                            parent_name="source"))
        self._fetch_adapter('target', 
                            self.config.target_adapter)

        self.target_adapter.load_config(self.config)

    def _set_connections(self):
        creds=deepcopy(self._credentials['source'])
        creds.pop('name')
        creds.pop('adapter')
        self.source_adapter.credentials=Credentials(**creds)

    def _load_credentials(self,credentials_path:str, 
                               source_profile:str, 
                               storage_profile:str)->None:
        logger.debug('loading credentials for adapters...')
        all_creds=load_from_file_or_path(credentials_path)
        requested_profiles=dict(source=source_profile,storage=storage_profile)

        for section in ('source','storage',):
            sections=f"{section}s" #pluralize ;)
            logger.debug(f'loading {sections} profile {requested_profiles[section]}...')
            profiles=get_config_value(all_creds,sections) 
            for profile in profiles:
                if profile['name'] == requested_profiles[section]:
                    self._credentials[section]=profile
            if section not in self._credentials.keys():
                message=f"{section} profile {requested_profiles[section]} not found in provided credentials."
                logger.error(message)
                raise AttributeError(message)    

    def _fetch_adapter(self,adapter_type:str,adapter_name:str):
        if adapter_type == 'source':
            adapters=source_adapters
        elif adapter_type == 'target':
            adapters=target_adapters
        elif adapter_type == 'storage':
            adapters=storage_adapters
        else:        
            raise KeyError(f'{adapter_type} is not a valid adapter type')
        logger.debug(f'loading {adapter_type} adapter...')
        try:
            classnamed= ''.join([part.capitalize() for part in adapter_name.split('_')] + ['Adapter'])
            
            self.__dict__[f"{adapter_type}_adapter"]=adapters.__dict__[f"{classnamed}"]()
            logger.debug(f'{adapter_type} adapter set to {classnamed}.')
        except KeyError as e:
            logger.error(f"failed to load config; {adapter_name} is not a valid {adapter_type} adapter.{e}")            
            raise e
        
    def _build_uncompiled_graphs(self)->List[networkx.Graph]:
        graph=SnowShuGraph()
        self._load_full_catalog()
        graph.build_graph(self.config,self.full_catalog)
        return graph.get_graphs()

