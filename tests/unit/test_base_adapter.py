import pytest
from unittest.mock import patch

import snowshu.core.models.materializations as mz
from snowshu.adapters import BaseSQLAdapter
from snowshu.adapters.source_adapters import BaseSourceAdapter
from snowshu.core.models import Relation
from snowshu.core.models.credentials import (ACCOUNT, DATABASE, HOST, PASSWORD,
                                             ROLE, SCHEMA, USER, Credentials)
from tests.common import rand_string


def rand_creds(args) -> Credentials:
    kwargs = dict(zip(args, [rand_string(10) for _ in range(len(args))]))
    return Credentials(**kwargs)


class StubbedAdapter(BaseSQLAdapter):

    REQUIRED_CREDENTIALS = (USER, PASSWORD, HOST)
    ALLOWED_CREDENTIALS = (ACCOUNT, SCHEMA)
    DATA_TYPE_MAPPINGS = dict()
    MATERIALIZATION_MAPPINGS = dict()


def test_sets_credentials():

    base = StubbedAdapter()

    with pytest.raises(KeyError):
        base.credentials = rand_creds((HOST,))

    with pytest.raises(KeyError):
        base.credentials = rand_creds((USER, PASSWORD, HOST, DATABASE,))

    base.credentials = rand_creds((USER, PASSWORD, HOST,))

    base.credentials = rand_creds((USER, PASSWORD, HOST, ACCOUNT,))


def test_default_conn_string():
    base = StubbedAdapter()
    base.dialect = 'postgres'

    base.REQUIRED_CREDENTIALS = (USER, PASSWORD, DATABASE, HOST)
    base.ALLOWED_CREDENTIALS = (ROLE, SCHEMA, ACCOUNT)

    creds = rand_creds((USER, PASSWORD, HOST, DATABASE, ACCOUNT,))
    base.credentials = creds

    assert base._build_conn_string(
    ) == f'postgres://{creds.user}:{creds.password}@{creds.host}/{creds.database}?account={creds.account}'


def test_build_catalog():
    config_patterns = [
        dict(database="snowshu_development",
             schema=".*",
             name="(?i)^.*(?<!_view)$"),
        dict(database="snowshu_development",
             schema="source_system",
             name="order_items_view")
    ]

    mock_filtered_schema = [
        BaseSQLAdapter._DatabaseObject("SOURCE_SYSTEM", Relation("snowshu_development", "source_system", "", None, None)),
        BaseSQLAdapter._DatabaseObject("Cased_Schema", Relation("snowshu_development", "Cased_Schema", "", None, None)),
    ]

    included_relations = [
        # included tables
        Relation("snowshu_development", "source_system", "fake_table_1", mz.TABLE, []),
        Relation("snowshu_development", "Cased_Schema", "fake_table_2", mz.TABLE, []),
        # included view
        Relation("snowshu_development", "source_system", "order_items_view", mz.VIEW, []),
    ]
    excluded_relations = [
        # excluded _view
        Relation("snowshu_development", "source_system", "some_other_view", mz.VIEW, []),
        Relation("snowshu_development", "Cased_Schema", "another_view", mz.VIEW, []),
    ]

    mock_relations = included_relations + excluded_relations
    def mock_get_relations_func(schema_obj: BaseSQLAdapter._DatabaseObject):
        return [r for r in mock_relations if schema_obj.full_relation.schema == r.schema]

    # stubbed version of the BaseSourceAdapter with the required class vars
    class StubbedSourceAdapter(BaseSourceAdapter):
        REQUIRED_CREDENTIALS = []
        ALLOWED_CREDENTIALS = []
        MATERIALIZATION_MAPPINGS = {}
        DATA_TYPE_MAPPINGS = {}
        SUPPORTED_SAMPLE_METHODS = []


    with patch("snowshu.adapters.BaseSQLAdapter._get_filtered_schemas", return_value=mock_filtered_schema) \
         , patch("snowshu.adapters.BaseSQLAdapter._get_relations_from_database", side_effect=mock_get_relations_func):
        adapter = StubbedAdapter()
        catalog = adapter.build_catalog(config_patterns, thread_workers=1)
        for r in excluded_relations:
            assert not r in catalog
        for r in included_relations:
            assert r in catalog
