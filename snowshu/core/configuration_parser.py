from pathlib import Path
import yaml
from typing import Union,TextIO,List,Optional
from snowshu.logger import Logger
from snowshu.configs import DEFAULT_THREAD_COUNT,DEFAULT_MAX_NUMBER_OF_OUTLIERS
from dataclasses import dataclass
from snowshu.core.sample_methods import SampleMethod, get_sample_method_from_kwargs
logger=Logger().logger

@dataclass 
class MatchPattern:

    @dataclass 
    class RelationPattern:
        relation_pattern:str

    @dataclass 
    class SchemaPattern:
        schema_pattern:str
        relations:List

    @dataclass 
    class DatabasePattern:
        database_pattern:str
        schemas:List

    databases:List[DatabasePattern]


@dataclass 
class SpecifiedMatchPattern():

    @dataclass 
    class RelationshipPattern:
        local_attribute:str 
        database_pattern:str
        schema_pattern:str
        relation_pattern:str
        remote_attribute:str 

    @dataclass
    class Relationships:
        bidirectional:Optional[List]
        directional:Optional[List]

    database_pattern:str
    schema_pattern:str
    relation_pattern:str
    unsampled:bool
    relationships:Relationships



@dataclass 
class Configuration():
    name:str
    version:str
    credpath:str
    short_description:str
    long_description:str
    threads:int
    source_profile:str
    target_adapter:str
    storage_profile:str
    include_outliers:bool
    max_number_of_outliers:int
    default_sample_method:SampleMethod
    default_sampling: List[MatchPattern]   
    specified_relations:List[SpecifiedMatchPattern]

class ConfigurationParser:
    
    def __init__(self):
        pass

    @staticmethod
    def from_file_or_path(loadable:Union[Path,str,TextIO])->Configuration:
        """ rips through a configuration and returns a configuration object"""
        try:
            with open(loadable) as f:
                logger.debug(f'loading from file {f.name}')
                loaded=yaml.safe_load(f)
        except TypeError:
            logger.debug('loading from file-like object...')
            loaded=yaml.safe_load(loadable)        


        logger.debug('Done loading.')
        try:
            replica_base=(  loaded['name'],
                            loaded['version'],
                            loaded['credpath'],
                            loaded.get('short_description',''),
                            loaded.get('long_description',''),
                            loaded.get('threads',DEFAULT_THREAD_COUNT),
                            loaded['source']['profile'],
                            loaded['target']['adapter'],
                            loaded['storage']['profile'],
                            loaded['source'].get('include_outliers',False),
                            loaded['source'].get('max_number_of_outliers',DEFAULT_MAX_NUMBER_OF_OUTLIERS),
                            get_sample_method_from_kwargs(**loaded['source']),
                            )
            
            default_sampling=MatchPattern([MatchPattern.DatabasePattern(database['name'],
                                                            [MatchPattern.SchemaPattern(schema['name'], 
                                                                                        [MatchPattern.RelationPattern(relation) for relation in schema['relations']]) 
                                                            for schema in database['schemas']]) for database in loaded['source']['default_sampling']['databases']])
                    
            specified_relations=[SpecifiedMatchPattern( rel['database'],
                                                        rel['schema'],
                                                        rel['relation'],
                                                        rel.get('unsampled',False),
                                                        SpecifiedMatchPattern.Relationships(
                                                            [SpecifiedMatchPattern.RelationshipPattern(
                                                                                                dsub['local_attribute'],
                                                                                                dsub['database'] if dsub['database'] != '' else None,
                                                                                                dsub['schema'] if dsub['schema'] != '' else None,
                                                                                                dsub['relation'],
                                                                                                dsub['remote_attribute']) for dsub in rel.get('relationships',dict()).get('directional',list())],

                                                            [SpecifiedMatchPattern.RelationshipPattern(
                                                                                                bsub['local_attribute'],
                                                                                                bsub['database'] if bsub['database'] != '' else None,
                                                                                                bsub['schema'] if bsub['schema'] != '' else None,
                                                                                                bsub['relation'],
                                                                                                bsub['remote_attribute']) for bsub in rel.get('relationships',dict()).get('bidirectional',list())])) for rel in loaded['source'].get('specified_relations',list())]

            return Configuration(*replica_base,
                                        default_sampling,
                                        specified_relations)
        except KeyError as e:
            message=f"Configuration missing required section: {e}."
            logger.critical(message)
            raise AttributeError(message)
