from typing import TYPE_CHECKING, List, Optional, Union
import json
import re
from sqlalchemy.types import JSON
import pandas as pd
from snowshu.configs import DEFAULT_MAX_NUMBER_OF_OUTLIERS
from snowshu.core.models import materializations as mz
from snowshu.core.models.attribute import Attribute
from snowshu.core.utils import correct_case, key_for_value
from snowshu.logger import Logger

if TYPE_CHECKING:
    from snowshu.core.configuration_parser import SpecifiedMatchPattern
    from snowshu.core.samplings.bases.base_sampling import BaseSampling
logger = Logger().logger


class Relation:
    _data: pd.DataFrame
    compiled_query: str
    core_query: str
    population_size: int
    sample_size: int
    source_extracted: bool = False
    target_loaded: bool = False
    sampling: Optional['BaseSampling']
    unsampled: bool = False
    include_outliers: bool = False
    max_number_of_outliers: int = DEFAULT_MAX_NUMBER_OF_OUTLIERS

    def __init__(self,  # noqa pylint: disable=too-many-arguments
                 database: str,
                 schema: str,
                 name: str,
                 materialization: Optional[mz.Materialization],
                 attributes: Optional[List[Attribute]]):

        self.database = database
        self.schema = schema
        self.name = name
        self.materialization = materialization
        self.attributes = attributes

    def __eq__(self, other):
        if not isinstance(other, Relation):
            return False

        return (self.name == other.name and
                self.schema == other.schema and
                self.database == other.database and
                self.materialization == other.materialization)

    def __hash__(self):
        return hash((self.name, self.schema, self.database, self.materialization))

    def __repr__(self) -> str:
        return f"<Relation object {self.database}.{self.schema}.{self.name}>"

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, val: pd.DataFrame) -> None:
        """ Setter for the relation's dataframe

            Adjusts data columns to match corrected attribute names and
            fixes mismatched datatypes
        """
        lowered_columns = [correct_case(col, False)
                           for col in val.columns.to_list()]
        attrs = [attr.name for attr in self.attributes]
        lowered_attrs = [correct_case(attr, False) for attr in attrs]
        val.columns = [attrs[lowered_attrs.index(
            col)] for col in lowered_columns]

        # handle the fact that pandas.read_sql may not preserve json type on load
        for attr in self.attributes:
            if isinstance(attr.data_type.sqlalchemy_type, JSON):
                transform_func = (lambda v: json.loads(v) if isinstance(v, str) else v)
                val[attr.name] = val[attr.name].transform(func=transform_func)

        self._data = val

    @property
    def dot_notation(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"

    @property
    def quoted_dot_notation(self) -> str:
        return '.'.join([self.quoted(getattr(self, x))
                        for x in ('database', 'schema', 'name',)])

    @property
    def star(self) -> str:
        attr_string = str()
        for attr in self.attributes:
            attr_string += f',{attr.name}\n'
        return attr_string[1:]

    # Relation.relation is confusing compared to Relation.name, but in other objects the
    # <database>.<schema>.<relation> convention makes this convenient.
    @property
    def relation(self) -> str:
        return self.name

    @relation.setter
    def relation(self, value: str) -> None:
        self.name = value

    @staticmethod
    def quoted(val: str) -> str:
        """Returns quoted value if appropriate."""
        return f'"{val}"' if all({val.isupper(),
                                  val.islower(), }) and ' ' not in val else val

    def scoped_cte(self, string: Optional[str] = None) -> str:
        """ returns a CTE name scoped to the relation.
            If _string_ is provided, this will be suffixed to the name."""
        return "__".join([self.database, self.schema, self.name, string])

    def typed_columns(self, data_type_mappings: dict) -> str:
        """generates the column section of a create statement in format <attr>
        <datatype>"""
        attr_string = str()
        for attr in self.attributes:
            attr_string += f',{self.quoted(attr.name)} {key_for_value(data_type_mappings, attr.data_type)}\n'
        return attr_string[1:]

    def lookup_attribute(self, attr: str) -> Union[Attribute, None]:
        """finds the attribute by name or returns None."""
        return next((a for a in self.attributes if a.name == attr), None)

    @property
    def is_view(self) -> bool:
        """convenience function for detecting if relation is a view."""
        return self.materialization == mz.VIEW


def lookup_single_relation(lookup: dict, relation_set: iter) -> Relation:
    """looks up a single relation by dict given the relation_set, returns None
    if not found.

    Args:
        lookup(dict) a dict of database, schema, relation keys
        relation_set(iter) any iterable of relations
    """
    logger.debug('looking for relation %s...', lookup)
    # flexibility to match other apis with either 'name' or 'relation'
    lookup['relation'] = lookup.get('relation', lookup.get('name'))
    found = next((rel for rel in relation_set if
                  rel.database == lookup['database']
                  and rel.schema == lookup['schema']
                  and rel.name == lookup['relation']), None)

    logger.debug('found %s.', found)
    return found


def lookup_relations(lookup: dict, relation_set: iter) -> List[Relation]:
    """Finds all relations that match regex patterns in given the relation_set,
    returns None if not found.

    Args:
        lookup(dict) a dict of database, schema, relation regex patterns
        relation_set(iter) any iterable of relations
    """
    logger.debug('looking for relations that match %s...', lookup)
    found = filter(lambda rel: single_full_pattern_match(rel, lookup), relation_set)
    logger.debug('found %s.', str(found))
    return list(found)


def single_full_pattern_match(rel: Relation,
                              pattern: Union[dict, 'SpecifiedMatchPattern']) -> bool:
    """determines if a relation matches a regex pattern.

    Pattern can be a dictionary of or a
    :class:`SpecifiedMatchPattern <snowshu.core.configuration_parser.SpecifiedMatchPattern>`.

    Args:
        rel: The :class:`Relation <snowshu.core.models.relation.Relation>` to be tested.
        pattern: Either a dict of database,schema,name(relation) or a
                    :class:`SpecifiedMatchPattern <snowshu.core.configuration_parser.SpecifiedMatchPattern>`.

    Returns:
        If the pattern matches the relation.
    """
    attributes = ('database', 'schema', 'name',)

    try:
        pattern = dict(database=pattern.database_pattern,
                       schema=pattern.schema_pattern,
                       name=pattern.relation_pattern)
    except AttributeError:
        pass
    if not all([pattern[attribute] for attribute in attributes]):
        return False
    return all([re.fullmatch(pattern[attr], rel.__dict__[attr])
                for attr in attributes])


def at_least_one_full_pattern_match(rel: Relation, patterns: iter) -> bool:
    """determines if a relation matches any of a collection of pattern
    dictionaries (database,schema,name)."""
    patterns = list(filter(lambda p: all(
        p[attr] for attr in ('database', 'schema', 'name',)), patterns))
    return any([single_full_pattern_match(rel, pattern)
                for pattern in patterns])
