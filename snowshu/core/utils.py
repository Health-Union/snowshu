import yaml
import os
import re
from pathlib import Path
from typing import Optional,Any,Union,TextIO
from snowshu.core.models.relation import Relation
from snowshu.logger import Logger
logger=Logger().logger

def lookup_relation(lookup:dict,relation_set:iter)->Relation:
    """ looks up a single relation by dict given the relation_set, returns None if not found.
        ARGS:
            lookup(dict) a dict of database, schema, relation keys
            relation_set(iter) any iterable of relations
    """
    logger.debug(f'looking for relation {lookup}...')
    found=next((rel for rel in relation_set if \
                                        rel.database==lookup['database'] \
                                    and rel.schema==lookup['schema'] \
                                    and rel.name==lookup['relation']),None)            

    logger.debug(f'found {found}.')
    return found

def single_full_pattern_match(rel:Relation,pattern:dict)->bool:
    """ determines if a relation matches a regex pattern dictionary of database,schema,name(relation)."""
    attributes=('database','schema','name',)
    return all([(lambda r,p : re.match(r,p)) (pattern[attr],rel.__dict__[attr],) for attr in attributes])

def at_least_one_full_pattern_match(rel:Relation,patterns:iter)->bool:
    """ determines if a relation matches any of a collection of pattern dictionaries (database,schema,name)."""
    return any([single_full_pattern_match(rel,pattern) for pattern in patterns])


def get_config_value(
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
            
def load_from_file_or_path(loadable:Union[Path,str,TextIO])->dict:
    try:
        with open(loadable) as f:
            logger.debug(f'loading from file {f.name}')
            loaded=yaml.safe_load(f)
    except TypeError:
        logger.debug('loading from file-like object...')
        loaded=yaml.safe_load(loadable)        
    logger.debug('Done loading.')
    return loaded
