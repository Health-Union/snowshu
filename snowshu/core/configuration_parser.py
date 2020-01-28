from pathlib import Path
from snowshu.core.utils import fetch_adapter
import yaml
from typing import Union, TextIO, List, Optional,Type
from snowshu.logger import Logger
from snowshu.configs import DEFAULT_THREAD_COUNT,DEFAULT_MAX_NUMBER_OF_OUTLIERS
from dataclasses import dataclass
from snowshu.core.samplings.utils import get_sampling_from_partial
from snowshu.core.models import Credentials
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
    sampling:Union['BaseSampling',None]
    include_outliers:Union[bool,None]
    relationships: Relationships

@dataclass
class AdapterProfile:
    name:str
    adapter:Union[Type['BaseSourceAdapter'],Type['BaseTargetAdapter'],Type['BaseStorageAdapter']]
    credentials:Union['Credentials',None]

@dataclass
class Configuration:
    name:str
    version:str
    credpath:str
    short_description:str
    long_description:str
    threads:int
    source_profile:AdapterProfile
    target_adapter:str
    storage_profile:AdapterProfile
    include_outliers:bool
    sampling:Type['BaseSampling']
    max_number_of_outliers:int
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
            def sampling_or_none(rel):
                if rel.get('sampling'):
                    return get_sampling_from_partial(rel['sampling'])
                
            return [SpecifiedMatchPattern( rel['database'],
                        rel['schema'],
                        rel['relation'],
                        rel.get('unsampled',False),
                        sampling_or_none(rel),
                        rel.get('include_outliers',None),
                        _build_relationships(rel)) for rel in specified_relations]
       
        def _build_adapter_profile(section:str,
                                   full_credentials:Union[str,'StringIO',dict])->AdapterProfile:

            profile=full_credentials[section]['profile']
            credentials=full_credentials['credpath']

            
            def get_credentials_dict(credentials:Union[str,'StringIO',dict])->dict:
                """loads credentials from the specified credentials path file.  
                
                Returns:
                    a formatted dict.
                """
                try:
                    assert isinstance(credentials,dict)
                    return credentials
                except AssertionError:
                    return yaml.load(credentials.read())
                except AttributeError:
                    with open(credentials,'r') as f:
                        return yaml.load(f.read())

            def lookup_profile_from_creds(creds_dict:dict,
                               profile:str,
                               section:str)->dict:
                """Finds the specified profile for the section in a given dict"""

                section=section if section.endswith('s') else section+'s'
                for creds_profile in creds_dict[section]:
                    if creds_profile['name'] == profile:
                        return creds_profile
                
            profile_dict=lookup_profile_from_creds(get_credentials_dict(credentials),
                                                   profile,
                                                   section)
            adapter=fetch_adapter(profile_dict['adapter'])
            
            del profile_dict['name']
            del profile_dict['adapter']
            credentials = Credentials(**profile_dict)

            return AdapterProfile(profile,
                                  adapter,
                                  credentials)
                                

        def _build_target(full_creds:dict)->AdapterProfile:
            return AdapterProfile(full_creds['target']['adapter'],
                                  fetch_adapter(full_creds['target']['adapter']),None)

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
                            _build_adapter_profile('source',loaded),
                            _build_target(loaded),
                            _build_adapter_profile('storage',loaded),
                            loaded['source'].get('include_outliers',False),
                            get_sampling_from_partial(loaded['source']['sampling']),
                            loaded['source'].get('max_number_of_outliers',DEFAULT_MAX_NUMBER_OF_OUTLIERS))

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
