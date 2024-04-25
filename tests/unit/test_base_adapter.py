import pytest
from unittest.mock import patch

import snowshu.core.models.materializations as mz
from snowshu.adapters import BaseSQLAdapter
from snowshu.adapters.source_adapters import BaseSourceAdapter
from snowshu.core.models import Relation
from snowshu.core.models.credentials import (ACCOUNT, DATABASE, HOST, PASSWORD,
                                             ROLE, SCHEMA, USER, Credentials)
from tests.common import rand_string


REQUIRED_CREDENTIALS = (USER, PASSWORD, HOST)
ALLOWED_CREDENTIALS = (ACCOUNT, SCHEMA)

class StubbedAdapter(BaseSQLAdapter):
    REQUIRED_CREDENTIALS = REQUIRED_CREDENTIALS
    ALLOWED_CREDENTIALS = ALLOWED_CREDENTIALS
    DATA_TYPE_MAPPINGS = dict()
    MATERIALIZATION_MAPPINGS = dict()

    def _get_all_databases(self):
        pass

    def _get_all_schemas(self, database):
        pass

    def _get_relations_from_database(self, schema_obj):
        pass

    def quoted(self, identifier):
        pass

@pytest.fixture
def stubbed_adapter():
    return StubbedAdapter()

def rand_creds(args) -> Credentials:
    kwargs = dict(zip(args, [rand_string(10) for _ in range(len(args))]))
    return Credentials(**kwargs)

@pytest.mark.parametrize("creds_args", [
    (HOST,),
    (USER, PASSWORD, HOST, DATABASE,)
])
def test_sets_credentials(stubbed_adapter, creds_args):
    with pytest.raises(KeyError):
        stubbed_adapter.credentials = rand_creds(creds_args)

@pytest.mark.parametrize("creds_args", [
    (USER, PASSWORD, HOST,),
    (USER, PASSWORD, HOST, ACCOUNT,)
])
def test_sets_credentials_success(stubbed_adapter, creds_args):
    stubbed_adapter.credentials = rand_creds(creds_args)
    stubbed_adapter.credentials = rand_creds((USER, PASSWORD, HOST,))
    stubbed_adapter.credentials = rand_creds((USER, PASSWORD, HOST, ACCOUNT,))

def test_default_conn_string(stubbed_adapter):
    stubbed_adapter.dialect = 'postgres'
    stubbed_adapter.REQUIRED_CREDENTIALS = (USER, PASSWORD, DATABASE, HOST)
    stubbed_adapter.ALLOWED_CREDENTIALS = (ROLE, SCHEMA, ACCOUNT)

    creds = rand_creds((USER, PASSWORD, HOST, DATABASE, ACCOUNT,))
    stubbed_adapter.credentials = creds

    assert stubbed_adapter._build_conn_string() == \
        f'postgres://{creds.user}:{creds.password}@{creds.host}/{creds.database}?account={creds.account}'

@pytest.mark.parametrize("config_patterns, mock_filtered_schema, included_relations, excluded_relations", [
    (
        [
            dict(database="snowshu_development", schema=".*", name="(?i)^.*(?<!_view)$"),
            dict(database="snowshu_development", schema="source_system", name="order_items_view")
        ],
        [
            BaseSQLAdapter._DatabaseObject("SOURCE_SYSTEM", Relation("snowshu_development", "source_system", "", None, None)),
            BaseSQLAdapter._DatabaseObject("Cased_Schema", Relation("snowshu_development", "Cased_Schema", "", None, None)),
        ],
        [
            Relation("snowshu_development", "source_system", "fake_table_1", mz.TABLE, []),
            Relation("snowshu_development", "Cased_Schema", "fake_table_2", mz.TABLE, []),
            Relation("snowshu_development", "source_system", "order_items_view", mz.VIEW, []),
        ],
        [
            Relation("snowshu_development", "source_system", "some_other_view", mz.VIEW, []),
            Relation("snowshu_development", "Cased_Schema", "another_view", mz.VIEW, []),
        ]
    )
])
def test_build_catalog(stubbed_adapter, config_patterns, mock_filtered_schema, included_relations, excluded_relations):
    mock_relations = included_relations + excluded_relations
    def mock_get_relations_func(schema_obj: StubbedAdapter._DatabaseObject):
        return [r for r in mock_relations if schema_obj.full_relation.schema == r.schema]

    with patch.object(StubbedAdapter, "_get_filtered_schemas", return_value=mock_filtered_schema), \
         patch.object(StubbedAdapter, "_get_relations_from_database", side_effect=mock_get_relations_func):
        catalog = stubbed_adapter.build_catalog(config_patterns, thread_workers=1)
        for r in excluded_relations:
            assert r not in catalog
        for r in included_relations:
            assert r in catalog