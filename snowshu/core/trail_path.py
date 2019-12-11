from concurrent.futures import ThreadPoolExecutor
import threading
import queue
from typing import Optional,Any,TextIO
from pathlib import Path
import os
import yaml
from typing import Union
import snowshu.source_adapters as adapters
from snowshu.logger import Logger
logger=Logger().logger

class TrailPath:

    source_adapter:adapters.BaseSourceAdapter

    def __init__(self):
        self._credentials=dict()
        self._connections=dict()

    def load_config(self,config:Union[Path,str,TextIO]):
        """ does all the initial work to make the resulting TrailPath object usable."""
        config=self._load_from_file_or_path(config)
        self._load_credentials(self._get_config_value(config,
                                                      "credpath",
                                                      "SNOWSHU_CREDENTIALS_FILEPATH"),
                                                      *[self._get_config_value(config[section], "profile") for section in ('source','target','storage',)])
        self.THREADS=int(self._get_config_value(config,"threads"))
        self._set_adapters()
        self._set_connections()
        

    def _load_full_catalog(self)->None:
        conn=self.connections['source']
        full_catalog=list()
        
        def accumulate_relation(queue:queue.Queue,accumulator:list):
            while not queue.empty():
                accumulator += list(self.source_adapter.get_relations_from_database(conn,queue.get()))
        
        relation_queue=queue.Queue()
        [relation_queue.put(db) for db in self.source_adapter.get_all_databases(conn)]
        with ThreadPoolExecutor(max_workers=self.THREADS) as executor:
            executor.submit(accumulate_relation, relation_queue, full_catalog)

        self.full_catalog=tuple(full_catalog)


    def _set_adapters(self):
        self._fetch_source_adapter(self._get_config_value(self._credentials['source'],'adapter',parent_name="source"))

    def _set_connections(self):
        self._connections['source']=self.source_adapter.get_connection(self._credentials['source'])

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
            

    @property
    def connections(self)->dict:
        return self._connections

    @connections.setter
    def connections(self,value)->None:
        raise NotImplementedError('connections cannot be explicitly set')           

