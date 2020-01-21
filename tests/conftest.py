import pytest
from io import StringIO
import tempfile
import copy
import json
import yaml
from snowshu.core.configuration_parser import ConfigurationParser
from tests.common import rand_string
from snowshu.core.models import Relation, Attribute
import snowshu.core.models.data_types as dt
import snowshu.core.models.materializations as mz
import networkx as nx
import pandas as pd
from dfmock import DFMock


CREDENTIALS = {
    "version": "1",
    "sources": [
        {
            "name": "default",
            "adapter": "snowflake",
            "account": "nwa1992.us-east-1",
            "password": "P@$$w0rD!",
            "database": "SNOWSHU_DEVELOPMENT",
            "user": "hanzgreuber"
        }
    ],
    "targets": [
        {
            "name": "default",
            "adapter": "postgres",
            "host": "localhost",
            "password": "postgres",
            "port": "5432",
            "user": "postgres"
        }
    ],
    "storages": [
        {
            "name": "default",
            "adapter": "aws-ecr",
            "access_key": "aosufipaufp",
            "access_key_id": "aiosfuaoifuafuiosf",
            "account": "sasquach.us-east-1"
        }
    ]
}


CONFIGURATION = {
    "version": "1",
    "credpath": "tests/assets/integration/credentials.yml",
    "name": "integration trail path",
    "short_description": "this is a sample with LIVE CREDS for integration",
    "long_description": "this is for testing against a live db",
    "threads": 15,
    "source": {
        "profile": "default",
        "default_sampling": {
            "databases": [
                {
                    "name": "SNOWSHU_DEVELOPMENT",
                    "schemas": [
                        {
                            "name": ".*",
                            "relations": [
                                "^(?!.+_VIEW).+$"
                            ]
                        }
                    ]
                }
            ]
        },
        "include_outliers": True,
        "sample_method": "bernoulli",
        "probability": 30,
        "specified_relations": [
            {
                "database": "SNOWSHU_DEVELOPMENT",
                "schema": "SOURCE_SYSTEM",
                "relation": "ORDERS",
                "unsampled": True
            },
            {
                "database": "SNOWSHU_DEVELOPMENT",
                "schema": "SOURCE_SYSTEM",
                "relation": "ORDER_ITEMS",
                "relationships": {
                    "bidirectional": [
                        {
                            "local_attribute": "PRODUCT_ID",
                            "database": "SNOWSHU_DEVELOPMENT",
                            "schema": "SOURCE_SYSTEM",
                            "relation": "PRODUCTS",
                            "remote_attribute": "ID"
                        }
                    ],
                    "directional": [
                        {
                            "local_attribute": "ORDER_ID",
                            "database": "SNOWSHU_DEVELOPMENT",
                            "schema": "SOURCE_SYSTEM",
                            "relation": "ORDERS",
                            "remote_attribute": "ID"
                        }
                    ]
                }
            }
        ]
    },
    "target": {
        "adapter": "postgres"
    },
    "storage": {
        "profile": "default"
    }
}


@pytest.fixture
def stub_creds():
    def _stub_creds():
        return copy.deepcopy(CREDENTIALS)
    return _stub_creds


@pytest.fixture
def stub_configs():
    def _stub_configs():
        return copy.deepcopy(CONFIGURATION)
    return _stub_configs


class RelationTestHelper:
    """builds a collection of different relations for testing"""

    def rand_relation_helper(self) -> dict:
        return dict(database=rand_string(10),
                    schema=rand_string(15),
                    materialization=mz.TABLE,
                    attributes=[]
                    )

    def __init__(self):
        self.downstream_relation = Relation(
            name='downstream_relation', **self.rand_relation_helper())
        self.upstream_relation = Relation(
            name='upstream_relation', **self.rand_relation_helper())
        self.iso_relation = Relation(
            name='iso_relation', **self.rand_relation_helper())
        self.birelation_left = Relation(
            name='birelation_left', **self.rand_relation_helper())
        self.birelation_right = Relation(
            name='birelation_right', **self.rand_relation_helper())
        self.view_relation = Relation(
            name='view_relation', **self.rand_relation_helper())
        self.bidirectional_key_left = rand_string(10),
        self.bidirectional_key_right = rand_string(8),
        self.directional_key = rand_string(15)

        # update specifics
        self.view_relation.materialization = mz.VIEW

        for n in ('downstream_relation', 'upstream_relation',):
            self.__dict__[n].attributes = [
                Attribute(self.directional_key, dt.INTEGER)]

        self.birelation_right.attributes = [
            Attribute(self.bidirectional_key_right, dt.VARCHAR)]
        self.birelation_left.attributes = [
            Attribute(self.bidirectional_key_left, dt.VARCHAR)]

        for r in ('downstream_relation', 'upstream_relation', 'iso_relation', 'birelation_left', 'birelation_right', 'view_relation',):
            self.__dict__[r].compiled_query = ''


@pytest.fixture
def stub_relation_set():
    return RelationTestHelper()


class AttributeTestHelper:

    def __init__(self):
        self.string_attribute = Attribute(
            name=rand_string(10), data_type=dt.VARCHAR)
        self.integer_attribute = Attribute(
            name=rand_string(10), data_type=dt.INTEGER)
        self.double_attribute = Attribute(
            name=rand_string(10), data_type=dt.DOUBLE)


@pytest.fixture
def stub_replica_configuration():
    return ConfigurationParser().from_file_or_path(StringIO(yaml.dump(CONFIGURATION)))


@pytest.fixture
def stub_relation():
    relation = RelationTestHelper()
    attrs = AttributeTestHelper()
    relation.iso_relation.attributes = [
        attrs.string_attribute, attrs.integer_attribute, attrs.double_attribute]

    return relation.iso_relation


@pytest.fixture
def stub_graph_set() -> tuple:
    """provides a collection of stubbed graphs with relations, and the raw values"""
    vals = RelationTestHelper()

    iso_graph = nx.DiGraph()
    iso_graph.add_node(vals.iso_relation)
    view_graph = nx.DiGraph()
    view_graph.add_node(vals.view_relation)

    dag = nx.DiGraph()
    dag.add_edge(vals.birelation_left, vals.birelation_right, direction='bidirectional',
                 local_attribute=vals.bidirectional_key_right, remote_attribute=vals.bidirectional_key_left)
    dag.add_edge(vals.upstream_relation, vals.downstream_relation, direction='directional',
                 local_attribute=vals.directional_key, remote_attribute=vals.directional_key)

    return [iso_graph, view_graph, dag], vals
