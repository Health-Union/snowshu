from pathlib import Path
import yaml
from typing import Union,TextIO
from snowshu.logger import Logger
from snowshu.utils import DEFAULT_THREAD_COUNT
from dataclasses import dataclass
logger=Logger().logger


##TODO: move all configs to class-based config       

@dataclass     
class ReplicaConfiguration:
    """top-level configs"""
    name:str
    short_description:str
    long_description:str
    threads:int
    source_name:str
    target_adapter:str
    storages_name:str

class ConfigurationParser:

    def __init__(self):
        pass

    def from_file_or_path(self,loadable:Union[Path,str,TextIO])->None:
        try:
            with open(loadable) as f:
                logger.debug(f'loading from file {f.name}')
                loaded=yaml.safe_load(f)
        except TypeError:
            logger.debug('loading from file-like object...')
            loaded=yaml.safe_load(loadable)        

        for k in loaded.keys():
            self.__dict__[k] = loaded[k]
        self._check_required_keys()
        logger.debug('Done loading.')
        self._standardize_keys()
        logger.debug('populated required values')
        self._replica_configuration = ReplicaConfiguration(self.name,
                                                            self.short_description,
                                                            self.long_description,
                                                            self.threads,
                                                            self.source['profile'],
                                                            self.target['adapter'],
                                                            self.storage['profile'])

    @property
    def replica_configuration(self)->ReplicaConfiguration:
        return self._replica_configuration


    def _check_required_keys(self)->None:
        for key in ('source','target','storage','name','version',):
            try:
                self.__dict__[key]
            except KeyError as e:
                message=f"Configuration missing required section {key}."
                logger.critical(message)
                raise AttributeError(message)

    def _standardize_keys(self)->None:
        """traverses attributes and adds the expected 
            default values"""
        for attr in ('long_description','short_description',):
            self.__dict__[attr]=getattr(self,attr,'')

        for rel in self.source['specified_relations']:
            rel['unsampled']=rel.get('unsampled',False)
            rel['relationships']=rel.get('relationships',dict(bidirectional=[],directional=[]))


