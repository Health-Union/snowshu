from snowshu.core.graph import SnowShuGraph
from snowshu.core.utils import get_config_value,\
load_from_file_or_path
from copy import deepcopy
from typing import TextIO,List
from snowshu.core.catalog import Catalog
from snowshu.core.models.credentials import Credentials
from pathlib import Path
from typing import Union
import snowshu.source_adapters as adapters
from snowshu.logger import Logger
from snowshu.core.models.relation import Relation
logger=Logger().logger

class TrailPath:

    source_adapter:adapters.BaseSourceAdapter
    dags:List[Relation]

    def __init__(self):
        self._credentials=dict()

    def load_config(self,config:Union[Path,str,TextIO]):
        """ does all the initial work to make the resulting TrailPath object usable."""
        config=load_from_file_or_path(config)
        self._load_credentials(get_config_value(config,
                                                      "credpath",
                                                      "SNOWSHU_CREDENTIALS_FILEPATH"),
                                                      *[get_config_value(config[section], "profile") for section in ('source','target','storage',)])
        self.source_configs=get_config_value(config,'source')
        
        self.THREADS=int(get_config_value(config,"threads"))
        self._load_adapters()
        self._set_connections()
   
    def load_dags(self)->None:
        graph=SnowShuGraph()
        graph.build_graph(self.source_configs,self.full_catalog)
        self.dags=graph.get_dags()        

    def _load_full_catalog(self)->None:
        catalog=Catalog(self.source_adapter,self.THREADS)
        catalog.load_full_catalog()
        self.full_catalog=catalog.catalog

    def _load_adapters(self):
        self._fetch_source_adapter(get_config_value(self._credentials['source'],'adapter',parent_name="source"))

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
        all_creds=load_from_file_or_path(credentials_path)
        requested_profiles=dict(source=source_profile,target=target_profile,storage=storage_profile)

        for section in ('source','target','storage',):
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

    def _fetch_source_adapter(self,adapter_name:str):
        logger.debug('loading source adapter...')
        try:
            classnamed= ''.join([part.capitalize() for part in adapter_name.split('_')] + ['Adapter'])
            self.source_adapter=adapters.__dict__[f"{classnamed}"]()
            logger.debug(f'source adapter set to {classnamed}.')
        except KeyError as e:
            logger.error(f"failed to load config; {adapter_name} is not a valid adapter.{e}")            
            raise e
        

            


