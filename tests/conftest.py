import copy
import json
import tempfile
from io import StringIO

import docker
import networkx as nx
import pandas as pd
import pytest
import yaml
from dfmock import DFMock

import snowshu.core.models.data_types as dt
import snowshu.core.models.materializations as mz
from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.models import Attribute, Relation
from tests.common import rand_string
from tests.conftest_modules.mock_docker_images import MockImageFactory
from tests.conftest_modules.test_configuration import CONFIGURATION
from tests.conftest_modules.test_credentials import CREDENTIALS


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


def sanitize_docker_environment():
    client=docker.from_env()
    def try_or_pass(statement,kwargs=dict()):
        try:
            statement(**kwargs)
        except:
            pass

    def is_snowshu_related_container(container)->bool:
        return any([val in container.name for val in ('snowshu_replica_',
            'integration-test',
            'snowshu_target',)])

    def is_snowshu_related_image(image)->bool:
        if len(image.tags) < 1:
            return False
        return any([val in image.tags[0] for val in ('snowshu_replica_',
            'integration-test',
            'snowshu_target',)])

    for container in filter(is_snowshu_related_container,client.containers.list()):
        try_or_pass(container.kill)
        try_or_pass(container.remove,dict(force=True))
    
    for image in filter(is_snowshu_related_image,client.images.list()):
        try_or_pass(client.images.remove,dict(image=image.tags[0],force=True))


@pytest.fixture
def docker_flush():
    sanitize_docker_environment()
    yield
    sanitize_docker_environment()

@pytest.fixture(scope="session")
def docker_flush_session():
    sanitize_docker_environment()
    yield
    sanitize_docker_environment()


@pytest.fixture
def mock_docker_image():
    return MockImageFactory()
