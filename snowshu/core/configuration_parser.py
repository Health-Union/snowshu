from pathlib import Path
import yaml
from typing import Union,TextIO
from snowshu.logger import Logger
from snowshu.utils import DEFAULT_THREAD_COUNT
logger=Logger().logger


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
        logger.debug('populated required source values')

    def _check_required_keys(self)->None:
        for key in ('source','target','storage',):
            try:
                self.__dict__[key]
            except KeyError as e:
                message=f"Configuration missing required section {key}."
                logger.critical(message)
                raise AttributeError(message)

    def _standardize_keys(self)->None:
        """traverses the source attributes and adds the expected 
            default values"""

        for rel in self.source['specified_relations']:
            rel['unsampled']=rel.get('unsampled',False)
            rel['relationships']=rel.get('relationships',dict(bidirectional=[],directional=[]))
            
