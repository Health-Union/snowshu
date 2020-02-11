from importlib import import_module
import yaml
import os
from pathlib import Path
from typing import Optional, Any, Union, TextIO, Type
from snowshu.logger import Logger
logger = Logger().logger


def case_insensitive_dict_value(dictionary,caseless_key)->Any:
    """finds a key in a dict without case sensitivity, returns value.

    Searches for the FIRST match (insensitive dict keys can have multiple matches) and returns that value.

    ARGS:
        - dictionary: The dictionary to traverse.
        - caseless_key: The key case-insensitive search the dictionary for.
    RETURNS:
        the value of insensitive key. Raises KeyError if not found.
    """
    lowered={key.lower():key for key in dictionary.keys()}
    return dictionary[lowered[caseless_key.lower()]]    



def key_for_value(dictionary, value):
    """finds the key for a given value in a dict."""
    return list(dictionary.keys())[list(dictionary.values()).index(value)]


def get_config_value(
        parent: dict,
        key: str,
        envar: Optional[str] = None,
        parent_name: Optional[str] = None) -> Any:
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


def load_from_file_or_path(loadable: Union[Path, str, TextIO]) -> dict:
    try:
        with open(loadable) as f:
            logger.debug(f'loading from file {f.name}')
            loaded = yaml.safe_load(f)
    except TypeError:
        logger.debug('loading from file-like object...')
        loaded = yaml.safe_load(loadable)
    logger.debug('Done loading.')
    return loaded



def fetch_adapter( name:str,
                   section:str)->Union[Type['BaseSourceAdapter'],Type['BaseTargetAdapter'],Type['BaseStorageAdapter']]:
    """Locates and returns the specified adapter.
    
    Args:
        name: The name of the adapter to look up.
        section: One of ('source','target','storage').
    Returns:
        The adapter if found, raises :class:`AdapterNotFound <snowshu.exceptions.AdapterNotFound>`.
    """
    try:
        return getattr(import_module(f'snowshu.adapters.{section}_adapters'),
                       name.capitalize() + 'Adapter')
    except AttributeError as e:
        logger.critical(f'No {section} adapter found by the name of {name}')
        raise e
