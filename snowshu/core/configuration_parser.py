from pathlib import Path
import yaml
from typing import Union,TextIO,List,Optional
from snowshu.logger import Logger
from snowshu.configs import DEFAULT_THREAD_COUNT
from dataclasses import dataclass
from snowshu.adapters.source_adapters.sample_methods import SampleMethod, get_sample_method_from_kwargs
logger=Logger().logger

@dataclass 
class MatchPattern:

    @dataclass 
    class RelationPattern:
        pattern:str

    @dataclass 
    class SchemaPattern:
        pattern:str
        relations:List

    @dataclass 
    class DatabasePattern:
        pattern:str
        schemas:List

    databases:List[DatabasePattern]


@dataclass 
class SpecifiedMatchPattern():

    @dataclass 
    class RelationshipPattern:
        local_attribute:str 
        database:str
        schema:str
        relation:str
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
    short_description:str
    long_description:str
    threads:int
    source_name:str
    target_adapter:str
    storages_name:str
    include_outliers:bool
    default_sampling_method:SampleMethod
    default_probability:int
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
            replica_base=(loaded['name'],
                                    loaded['version'],
                                    loaded.get('short_description',''),
                                    loaded.get('long_description',''),
                                    loaded.get('threads',DEFAULT_THREAD_COUNT),
                                    loaded.get('include_outliers',False),
                                    get_sample_method_from_kwargs(**loaded['source']),
                                    loaded['source']['profile'],
                                    loaded['target']['adapter'],
                                    loaded['storage']['profile'],)

            default_sampling=MatchPattern([MatchPattern.DatabasePattern(database,
                                                            [MatchPattern.SchemaPattern(schema, 
                                                                                        [MatchPattern.RelationPattern(relation) for relation in schema]) 
                                                            for schema in database]) for database in loaded['source']['default_sampling']])
                    
            specified_relations=[SpecifiedMatchPattern( rel['database'],
                                                        rel['schema'],
                                                        rel['relation'],
                                                        rel.get('unsampled',False),
                                                        SpecifiedMatchPattern.Relationships(
                                                            [SpecifiedMatchPattern.RelationshipPattern(
                                                                                                dsub['local_attribute'],
                                                                                                dsub['database'],
                                                                                                dsub['schema'],
                                                                                                dsub['relation'],
                                                                                                dsub['remote_attribute']) for dsub in rel.get('directional',list())],

                                                            [SpecifiedMatchPattern.RelationshipPattern(
                                                                                                bsub['local_attribute'],
                                                                                                bsub['database'],
                                                                                                bsub['schema'],
                                                                                                bsub['relation'],
                                                                                                bsub['remote_attribute']) for bsub in rel.get('bidirectional',list())])) for rel in loaded['source'].get('specified_relationships',list())]

            
            return Configuration(*replica_base,
                                        default_sampling,
                                        specified_relations)
        except KeyError as e:
            message=f"Configuration missing required section {key}."
            logger.critical(message)
            raise AttributeError(message)
