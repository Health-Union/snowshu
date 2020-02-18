from pathlib import Path
from snowshu.core.utils import fetch_adapter,correct_case
import yaml
from typing import Union, TextIO, List, Optional,Type,Any
from snowshu.logger import Logger
from snowshu.configs import DEFAULT_THREAD_COUNT, \
DEFAULT_MAX_NUMBER_OF_OUTLIERS, \
DEFAULT_PRESERVE_CASE
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
    adapter:Union[Type['BaseSourceAdapter'],Type['BaseTargetAdapter']]

@dataclass
class Configuration:
    name:str
    version:str
    credpath:str
    short_description:str
    long_description:str
    threads:int
    preserve_case:bool
    source_profile:AdapterProfile
    target_profile:AdapterProfile
    include_outliers:bool
    sampling:Type['BaseSampling']
    max_number_of_outliers:int
    general_relations: List[MatchPattern]   
    specified_relations:List[SpecifiedMatchPattern]


class ConfigurationParser:

    def _get_dict_from_anything(self, dict_like_object:Union[str,'StringIO',dict])->dict:
        """Returns dict from path, io object or dict.  
        
        Returns:
            a formatted dict.
        """
        try:
            assert isinstance(dict_like_object,dict)
            return dict_like_object
        except AssertionError:
            try:
                return yaml.safe_load(dict_like_object.read())
            except AttributeError:
                with open(dict_like_object,'r') as f:
                    return yaml.safe_load(f.read())

    def _set_default(self,
                     dict_from:dict,
                     attr:str,
                     default:Any='')->None:
        """sets default for a given dict key."""
            
        dict_from[attr]=dict_from.get(attr,default)

    def case(self,val:str)->str:
        """Does the up-front case correction.
        SnowShu uses the default source case as the "case insensitive" fold.
        Args:
            val: The value to case correct.
        Returns:
            The corrected string.
        """
        if self.preserve_case:
            return val
        else:
            return correct_case(val,self.default_case == 'upper')

    ## TODO: now that there's an instance dependency on preserve_case and correct_case, 
    ## this should be migrated to an init
    def from_file_or_path(self, loadable: Union[Path, str, TextIO]) -> Configuration:
        """rips through a configuration and returns a configuration object."""
        logger.debug(f'loading credentials...')
        loaded=self._get_dict_from_anything(loadable)
        logger.debug('Done loading.')

        ## we need the source adapter first to case-correct everything else
        self._set_default(loaded,'preserve_case',DEFAULT_PRESERVE_CASE)
        self.preserve_case=loaded['preserve_case']
        source_adapter_profile=self._build_adapter_profile('source',loaded)
        self.default_case = source_adapter_profile.adapter.DEFAULT_CASE

        def case(val:str)->str:
            return self.case(val)
            

        ## make sure no empty sections
        try:
            [loaded[section].keys() for section in ('source','target',)]
        except TypeError as e:
            raise KeyError(f'missing config section or section is none: {e}.')

        ## set defaults
        [self._set_default(loaded,attr) for attr in ('short_description','long_description',)]
        self._set_default(loaded,'threads',DEFAULT_THREAD_COUNT)
        self._set_default(loaded['source'],'include_outliers',False)
        self._set_default(loaded['source'],'max_number_of_outliers',DEFAULT_MAX_NUMBER_OF_OUTLIERS)

        try:
            replica_base = (loaded['name'],
                            loaded['version'],
                            loaded['credpath'],
                            loaded['short_description'],
                            loaded['long_description'],
                            loaded['threads'],
                            self.preserve_case,
                            source_adapter_profile,
                            self._build_target(loaded),
                            loaded['source']['include_outliers'],
                            get_sampling_from_partial(loaded['source']['sampling']),
                            loaded['source']['max_number_of_outliers'])


            general_relations=MatchPattern([MatchPattern.DatabasePattern(case(database['pattern']),
                                                            [MatchPattern.SchemaPattern(case(schema['pattern']), 
                                                                                        [MatchPattern.RelationPattern(case(relation)) for relation in schema['relations']]) 
                                                            for schema in database['schemas']]) for database in loaded['source']['general_relations']['databases']])
                    
            specified_relations=self._build_specified_relations(loaded['source'])

            return Configuration(*replica_base,
                                 general_relations,
                                 specified_relations)
        except KeyError as e:
            message = f"Configuration missing required section: {e}."
            logger.critical(message)
            raise AttributeError(message)

        
    
    def _build_relationships(self,specified_pattern:dict)->SpecifiedMatchPattern.Relationships:
        
        def build_relationship(sub)->SpecifiedMatchPattern.RelationshipPattern:
            return SpecifiedMatchPattern.RelationshipPattern(
                self.case(sub['local_attribute']),
                self.case(sub['database']) if sub['database'] != '' else None,
                self.case(sub['schema']) if sub['schema'] != '' else None,
                self.case(sub['relation']),
                self.case(sub['remote_attribute']))

        relationships = specified_pattern.get('relationships',dict())
        directional=relationships.get('directional',list())
        bidirectional=relationships.get('bidirectional',list())
        return SpecifiedMatchPattern.Relationships(
                [build_relationship(rel) for rel in bidirectional],
                [build_relationship(rel) for rel in directional])

        
    def _build_specified_relations(self,source_config:dict)->SpecifiedMatchPattern:
        
        specified_relations=source_config.get('specified_relations',list())
        def sampling_or_none(rel):
            if rel.get('sampling'):
                return get_sampling_from_partial(rel['sampling'])
            
        return [SpecifiedMatchPattern( self.case(rel['database']),
                    self.case(rel['schema']),
                    self.case(rel['relation']),
                    rel.get('unsampled',False),
                    sampling_or_none(rel),
                    rel.get('include_outliers',None),
                    self._build_relationships(rel)) for rel in specified_relations]
    
    def _build_adapter_profile(self,section:str,
                               full_configs:Union[str,'StringIO',dict])->AdapterProfile:
        profile=full_configs[section]['profile']
        credentials=full_configs['credpath']
        
        def lookup_profile_from_creds(creds_dict:dict,
                           profile:str,
                           section:str)->dict:
            """Finds the specified profile for the section in a given dict"""
            section=section if section.endswith('s') else section+'s'
            try:
                for creds_profile in creds_dict[section]:
                    if creds_profile['name'] == profile:
                        return creds_profile
                raise KeyError(profile)
            except KeyError as e:
                raise ValueError(f'Credentials missing required section: {e}')
        profile_dict=lookup_profile_from_creds(self._get_dict_from_anything(credentials),
                                               profile,
                                               section)

        adapter=fetch_adapter(profile_dict['adapter'],section)

        del profile_dict['name']
        del profile_dict['adapter']
        adapter=adapter()
        adapter.credentials=Credentials(**profile_dict)
        return AdapterProfile(profile,
                              adapter)
                            
    
    def _build_target(self,full_creds:dict)->AdapterProfile:
        adapter=fetch_adapter(full_creds['target']['adapter'],'target')()
        adapter.replica_meta={attr:full_creds[attr] for attr in ('name','short_description','long_description',)}
        return AdapterProfile(full_creds['target']['adapter'],
                              adapter)


