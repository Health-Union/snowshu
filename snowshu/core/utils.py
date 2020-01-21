import yaml
import os
import re
from pathlib import Path
from typing import Optional, Any, Union, TextIO
from snowshu.logger import Logger
logger = Logger().logger


def key_for_value(dictionary, value):
    """finds the key for a given value in a dict"""
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
