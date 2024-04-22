import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, TextIO, Type, Union
import logging


import jsonschema
import yaml
from jsonschema.exceptions import ValidationError

from snowshu.configs import (DEFAULT_MAX_NUMBER_OF_OUTLIERS,
                             DEFAULT_PRESERVE_CASE, DEFAULT_THREAD_COUNT)
from snowshu.core.models import Credentials, materializations
from snowshu.core.samplings.utils import get_sampling_from_partial
from snowshu.core.utils import correct_case, fetch_adapter

if TYPE_CHECKING:
    from io import StringIO
    from snowshu.core.samplings.bases.base_sampling import BaseSampling
    from snowshu.adapters.source_adapters.base_source_adapter import BaseSourceAdapter
    from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter

logger = logging.getLogger(__name__)

TEMPLATES_PATH = Path(os.path.dirname(__file__)).parent / 'templates'
REPLICA_JSON_SCHEMA = TEMPLATES_PATH / 'replica_schema.json'
CREDENTIALS_JSON_SCHEMA = TEMPLATES_PATH / 'credentials_schema.json'


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
    class PolymorphicRelationshipPattern(RelationshipPattern):
        local_type_attribute: str
        local_type_overrides: dict

    @dataclass
    class Relationships:
        bidirectional: List['RelationshipPattern']
        directional: List['RelationshipPattern']
        polymorphic: List['PolymorphicRelationshipPattern']

    database_pattern: str
    schema_pattern: str
    relation_pattern: str
    unsampled: bool
    sampling: Union['BaseSampling', None]
    include_outliers: Union[bool, None]
    relationships: Relationships


@dataclass
class AdapterProfile:
    name: str
    adapter: Union["BaseSourceAdapter", "BaseTargetAdapter"]


@dataclass
class Configuration:  # noqa pylint: disable=too-many-instance-attributes
    name: str
    version: str
    credpath: str
    short_description: str
    long_description: str
    threads: int
    preserve_case: bool
    source_profile: AdapterProfile
    target_profile: AdapterProfile
    include_outliers: bool
    sampling: Type['BaseSampling']
    max_number_of_outliers: int
    general_relations: List[MatchPattern]
    specified_relations: List[SpecifiedMatchPattern]


class ConfigurationParser:
    default_case: str = None
    preserve_case: bool = False


    def get_dict_from_anything(self,
                                dict_like_object: Union[str, 'StringIO', dict],
                                schema_path: Path) -> dict:
        """Returns dict from path, io object or dict.

        Returns:
            a formatted dict.
        """
        # TODO validation against the schema should happen in all cases
        try:
            assert isinstance(dict_like_object, dict)
            return dict_like_object
        except AssertionError:
            try:
                return yaml.safe_load(dict_like_object.read())
            except AttributeError:
                with open(dict_like_object, 'r') as stream:  # noqa pylint: disable=unspecified-encoding
                    instance = yaml.safe_load(stream.read())
                return self._verify_schema(instance, Path(dict_like_object), schema_path)

    @staticmethod
    def _verify_schema(instance,
                       file_path: Path,
                       schema_path: Path):
        logger.debug("Parsing file at %s", file_path)
        with open(schema_path) as schema_file:  # noqa pylint: disable=unspecified-encoding
            schema = yaml.safe_load(schema_file.read())

        try:
            jsonschema.validate(instance=instance, schema=schema)
        except ValidationError as exc:
            logger.error('Invalid object %s', exc)
            raise exc

        return instance

    @staticmethod
    def _set_default(dict_from: dict,
                     attr: str,
                     default: Any = '') -> None:
        """sets default for a given dict key."""

        dict_from[attr] = dict_from.get(attr, default)

    def case(self, val: str) -> str:
        """Does the up-front case correction.
        SnowShu uses the default source case as the "case insensitive" fold.
        Args:
            val: The value to case correct.
        Returns:
            The corrected string.
        """
        if self.preserve_case:
            return val

        return correct_case(val, self.default_case == 'upper')

    def _build_general_relations(self, loaded: dict) -> MatchPattern:
        pattern_list = []
        for database in loaded["source"]["general_relations"]["databases"]:
            database_pattern = self.case(database["pattern"])

            schema_patterns = []
            for schema in database["schemas"]:
                schema_pattern = self.case(schema["pattern"])
                relation_patterns = [
                    MatchPattern.RelationPattern(self.case(relation))
                    for relation in schema["relations"]
                ]
                schema_patterns.append(
                    MatchPattern.SchemaPattern(schema_pattern, relation_patterns)
                )

            pattern_list.append(
                MatchPattern.DatabasePattern(database_pattern, schema_patterns)
            )

        return MatchPattern(pattern_list)

    # TODO: now that there's an instance dependency on preserve_case and correct_case,
    # this should be migrated to an init
    def from_file_or_path(
            self, loadable: Union[Path, str, TextIO]) -> Configuration:
        """rips through a configuration and returns a configuration object."""
        logger.debug('loading configuration...')
        loaded = self.get_dict_from_anything(loadable, REPLICA_JSON_SCHEMA)
        logger.debug('Done loading.')

        # we need the source adapter first to case-correct everything else
        self._set_default(loaded, 'preserve_case', DEFAULT_PRESERVE_CASE)
        self.preserve_case = loaded['preserve_case']
        source_adapter_profile = self._build_adapter_profile('source', loaded)

        self.default_case = source_adapter_profile.adapter.DEFAULT_CASE

        # Pass materialization mapping to source adapter
        self._set_default(loaded['source'], 'copy_views_as_tables', True)
        if loaded['source']['copy_views_as_tables']:
            materialization_mappings = {
                "BASE TABLE": materializations.TABLE,
                "VIEW": materializations.TABLE
            }
        else:
            materialization_mappings = {
                "BASE TABLE": materializations.TABLE,
                "VIEW": materializations.VIEW
            }
        source_adapter_profile.adapter.MATERIALIZATION_MAPPINGS = materialization_mappings

        # make sure no empty sections and section is not None
        for section in ('source', 'target',):
            if section not in loaded or not isinstance(loaded[section], dict):
                raise KeyError(f"Missing or invalid config section: '{section}'.")

        # set defaults
        for attr in ('short_description', 'long_description',):
            self._set_default(loaded, attr)
        self._set_default(
            loaded,
            'threads',
            DEFAULT_THREAD_COUNT)
        self._set_default(
            loaded['source'],
            'include_outliers',
            False)
        self._set_default(
            loaded['source'],
            'max_number_of_outliers',
            DEFAULT_MAX_NUMBER_OF_OUTLIERS)

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
                            get_sampling_from_partial(
                                loaded['source']['sampling']),
                            loaded['source']['max_number_of_outliers'])

            general_relations = self._build_general_relations(loaded)
            specified_relations = self._build_specified_relations(loaded['source'])

            return Configuration(*replica_base,
                                 general_relations,
                                 specified_relations)
        except KeyError as err:
            message = f"Configuration missing required section: {err}."
            logger.critical(message)
            raise AttributeError(message) from err

    def _build_relationships(
            self,
            specified_pattern: dict) -> SpecifiedMatchPattern.Relationships:

        def build_relationship(
                sub) -> SpecifiedMatchPattern.RelationshipPattern:
            return SpecifiedMatchPattern.RelationshipPattern(
                self.case(sub['local_attribute']),
                self.case(sub['database']) if sub['database'] != '' else None,
                self.case(sub['schema']) if sub['schema'] != '' else None,
                self.case(sub['relation']),
                self.case(sub['remote_attribute']))

        def build_polymorphic_relationship(
                sub) -> SpecifiedMatchPattern.PolymorphicRelationshipPattern:
            override_dict = {}
            if 'local_type_overrides' in sub:
                for override in sub['local_type_overrides']:
                    key = '.'.join([
                        self.case(override['database']),
                        self.case(override['schema']),
                        self.case(override['relation'])
                    ])
                    value = override['override_value']
                    override_dict[key] = value

            return SpecifiedMatchPattern.PolymorphicRelationshipPattern(
                self.case(sub['local_attribute']),
                self.case(sub['database']) if sub['database'] != '' else None,
                self.case(sub['schema']) if sub['schema'] != '' else None,
                self.case(sub['relation']),
                self.case(sub['remote_attribute']),
                self.case(sub['local_type_attribute']) if 'local_type_attribute' in sub else None,
                override_dict
            )

        relationships = specified_pattern.get('relationships', {})
        directional = relationships.get('directional', list())
        bidirectional = relationships.get('bidirectional', list())
        polymorphic = relationships.get('polymorphic', list())
        return SpecifiedMatchPattern.Relationships(
            [build_relationship(rel) for rel in bidirectional],
            [build_relationship(rel) for rel in directional],
            [build_polymorphic_relationship(rel) for rel in polymorphic]
        )

    def _build_specified_relations(
            self, source_config: dict) -> SpecifiedMatchPattern:

        specified_relations = source_config.get('specified_relations', list())

        def sampling_or_none(rel):
            if rel.get('sampling'):
                return get_sampling_from_partial(rel['sampling'])
            return None

        return [SpecifiedMatchPattern(self.case(rel['database']),
                                      self.case(rel['schema']),
                                      self.case(rel['relation']),
                                      rel.get('unsampled', False),
                                      sampling_or_none(rel),
                                      rel.get('include_outliers', None),
                                      self._build_relationships(rel)) for rel in specified_relations]

    def _build_adapter_profile(self,
                               section: str,
                               full_configs: Union[str, 'StringIO', dict]) -> AdapterProfile:
        profile = full_configs[section]['profile']
        credentials = full_configs['credpath']

        def lookup_profile_from_creds(creds_dict: dict,
                                      profile: str,
                                      section: str) -> dict:
            """Finds the specified profile for the section in a given dict"""
            section = section if section.endswith('s') else section + 's'
            try:
                for creds_profile in creds_dict[section]:
                    if creds_profile['name'] == profile:
                        return creds_profile
                raise KeyError(profile)
            except KeyError as err:
                raise ValueError(f'Credentials missing required section: {err}') from err
        try:
            profile_dict = lookup_profile_from_creds(self.get_dict_from_anything(credentials,
                                                                                  CREDENTIALS_JSON_SCHEMA),
                                                     profile,
                                                     section)
        except FileNotFoundError as err:
            err.strerror = "Credentials specified in replica.yml not found. " + err.strerror
            raise

        adapter = fetch_adapter(profile_dict['adapter'], section)

        del profile_dict['name']
        del profile_dict['adapter']
        adapter = adapter()
        adapter.credentials = Credentials(**profile_dict)
        return AdapterProfile(profile,
                              adapter)

    @staticmethod
    def _build_target(full_config: dict) -> AdapterProfile:
        adapter_type = fetch_adapter(full_config['target']['adapter'], 'target')
        adapter_args = full_config['target'].get('adapter_args')
        if not adapter_args:
            adapter_args = {}
        metadata = {
            attr: full_config[attr] for attr in (
                'name', 'short_description', 'long_description',)}
        metadata['config_json'] = json.dumps(full_config)
        adapter_args['replica_metadata'] = metadata
        adapter = adapter_type(**adapter_args)
        return AdapterProfile(full_config['target']['adapter'],
                              adapter)
