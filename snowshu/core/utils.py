import os
import re
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, TextIO, Type, Union

import yaml

from snowshu.logger import Logger

if TYPE_CHECKING:
    from snowshu.adapters.base_sql_adapter import BaseSQLAdapter
    from snowshu.adapters.source_adapters.base_source_adapter import BaseSourceAdapter
    from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter


logger = Logger().logger


def correct_case(val: str, upper: bool = True):
    """ Returns the case corrected value based on general sql identifier rules

        If the value is entirely one case, made up of only word characters
        and doesn't begin with a number, we can conform the case

        ARGS:
            - val: string that is the value to correct case for
            - upper: flag to determine the case to conform to. Defaults to True (uppercase)
        RETURNS:
            the case corrected value
    """
    if any({val.isupper(), val.islower()}) and \
            re.fullmatch(r'^\w*$', val) and \
            not re.fullmatch(r'^[0-9].*', val):
        val = val.upper() if upper else val.lower()
    return val


def case_insensitive_dict_value(dictionary, caseless_key) -> Any:
    """finds a key in a dict without case sensitivity, returns value.

    Searches for the FIRST match (insensitive dict keys can have multiple matches) and returns that value.

    ARGS:
        - dictionary: The dictionary to traverse.
        - caseless_key: The key case-insensitive search the dictionary for.
    RETURNS:
        the value of insensitive key. Raises KeyError if not found.
    """
    lowered = {key.lower(): key for key in dictionary.keys()}
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
    except KeyError as err:
        if envar is not None and os.getenv(envar) is not None:
            return os.getenv(envar)

        message = (f'Config issue: missing required attribute'
                   f'{key + " from object " + parent_name if parent_name is not None else key}.')
        logger.error(message)
        raise err


def load_from_file_or_path(loadable: Union[Path, str, TextIO]) -> dict:
    try:
        with open(loadable) as file_obj:
            logger.debug('loading from file %s', file_obj.name)
            loaded = yaml.safe_load(file_obj)
    except TypeError:
        logger.debug('loading from file-like object...')
        loaded = yaml.safe_load(loadable)
    logger.debug('Done loading.')
    return loaded


def fetch_adapter(name: str,
                  section: str) -> Union[Type['BaseSourceAdapter'], Type['BaseTargetAdapter'], Type['BaseSQLAdapter']]:
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
    except AttributeError as err:
        logger.critical('No %s adapter found by the name of %s', section, name)
        raise err
