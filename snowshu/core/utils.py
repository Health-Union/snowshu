import re
from snowshu.core.relation import Relation
from snowshu.logger import Logger
logger=Logger().logger

def lookup_relation(lookup:dict,relation_set:iter)->Relation:
    """ looks up a single relation by dict given the relation_set, returns None if not found.
        ARGS:
            lookup(dict) a dict of database, schema, relation keys
            relation_set(iter) any iterable of relations
    """
    logger.info(f'looking for relation {lookup} in set {relation_set}...')
    found=next((rel for rel in relation_set if \
                                        rel.database==lookup['database'] \
                                    and rel.schema==lookup['schema'] \
                                    and rel.name==lookup['relation']),None)            

    logger.info(f'found {found}.')
    return found

def single_full_pattern_match(rel:Relation,pattern:dict)->bool:
    """ determines if a relation matches a regex pattern dictionary of database,schema,name(relation)."""
    attributes=('database','schema','name',)
    return all([(lambda r,p : re.match(r,p)) (pattern[attr],rel.__dict__[attr],) for attr in attributes])

def at_least_one_full_pattern_match(rel:Relation,patterns:iter)->bool:
    """ determines if a relation matches any of a collection of pattern dictionaries (database,schema,name)."""
    return any([single_full_pattern_match(rel,pattern) for pattern in patterns])
