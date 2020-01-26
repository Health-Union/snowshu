from pathlib import Path
import yaml
from typing import Union, TextIO, List, Optional,Type
from snowshu.logger import Logger
from snowshu.configs import DEFAULT_THREAD_COUNT,DEFAULT_MAX_NUMBER_OF_OUTLIERS
from dataclasses import dataclass
from snowshu.core.sampling.sample_methods import SampleMethod, get_sample_method_from_kwargs
from snowshu.core.samplings import BaseSampling
from snowshu.samplings.utils import get_sampling_from_partial
logger = Logger().logger


@dataclass
class MatchPattern:

    @dataclass
    class RelationPattern:
        relation_pattern: str

    @dataclass
    class SchemaPattern:
        schema_pattern: str
        relations: List

    @dataclass
    class DatabasePattern:
        database_pattern: str
        schemas: List

    databases: List[DatabasePattern]


@dataclass
class SpecifiedMatchPattern():

    @dataclass
    class RelationshipPattern:
        local_attribute: str
        database_pattern: str
        schema_pattern: str
        relation_pattern: str
        remote_attribute: str

    @dataclass
    class Relationships:
        bidirectional: List['RelationshipPattern']
        directional: List['RelationshipPattern']

    database_pattern: str
    schema_pattern: str
    relation_pattern: str
    unsampled: bool
    sampling:Union[BaseSampling,None]
    relationships: Relationships

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
    sampling:Type[BaseSampling]
    max_number_of_outliers:int
    default_sample_method:SampleMethod
    general_relations: List[MatchPattern]   
    specified_relations:List[SpecifiedMatchPattern]


class ConfigurationParser:

    def __init__(self):
        pass

    @staticmethod
    def from_file_or_path(loadable: Union[Path, str, TextIO]) -> Configuration:
        """rips through a configuration and returns a configuration object."""


        def _build_relationships(specified_pattern:dict)->SpecifiedMatchPattern.Relationships:
            
            def build_relationship(sub)->SpecifiedMatchPattern.RelationshipPattern:
                return SpecifiedMatchPattern.RelationshipPattern(
                    sub['local_attribute'],
                    sub['database'] if sub['database'] != '' else None,
                    sub['schema'] if sub['schema'] != '' else None,
                    sub['relation'],
                    sub['remote_attribute'])

            relationships = specified_pattern.get('relationships',dict())
            directional=relationships.get('directional',list())
            bidirectional=relationships.get('bidirectional',list())
            return SpecifiedMatchPattern.Relationships(
                    [build_relationship(rel) for rel in bidirectional],
                    [build_relationship(rel) for rel in directional])
            

        def _build_specified_relations(source_config:dict)->SpecifiedMatchPattern:
            
            specified_relations=source_config.get('specified_relations',list())
            return [SpecifiedMatchPattern( rel['database'],
                        rel['schema'],
                        rel['relation'],
                        rel.get('unsampled',False),
                        rel.get('Sampling',None),
                        _build_relationships(rel)) for rel in specified_relations]

        try:
            with open(loadable) as f:
                logger.debug(f'loading from file {f.name}')
                loaded = yaml.safe_load(f)
        except TypeError:
            logger.debug('loading from file-like object...')
            loaded = yaml.safe_load(loadable)

        logger.debug('Done loading.')
        try:
            replica_base = (loaded['name'],
                            loaded['version'],
                            loaded['credpath'],
                            loaded.get('short_description', ''),
                            loaded.get('long_description', ''),
                            loaded.get('threads', DEFAULT_THREAD_COUNT),
                            loaded['source']['profile'],
                            loaded['target']['adapter'],
                            loaded['storage']['profile'],
                            loaded['source'].get('include_outliers',False),
                            get_sampling_from_partial(loaded['source']['sampling']),
                            loaded['source'].get('max_number_of_outliers',DEFAULT_MAX_NUMBER_OF_OUTLIERS),
                            get_sample_method_from_kwargs(**loaded['source']),
                            )
            
            general_relations=MatchPattern([MatchPattern.DatabasePattern(database['pattern'],
                                                            [MatchPattern.SchemaPattern(schema['pattern'], 
                                                                                        [MatchPattern.RelationPattern(relation) for relation in schema['relations']]) 
                                                            for schema in database['schemas']]) for database in loaded['source']['general_relations']['databases']])
                    
            specified_relations=_build_specified_relations(loaded['source'])

            return Configuration(*replica_base,
                                 general_relations,
                                 specified_relations)
        except KeyError as e:
            message = f"Configuration missing required section: {e}."
            logger.critical(message)
            raise AttributeError(message)
