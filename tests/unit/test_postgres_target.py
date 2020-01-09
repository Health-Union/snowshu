import pytest
import mock
from sqlalchemy import create_engine
from tests.common import rand_string
from snowshu.adapters.target_adapters import PostgresAdapter


def test_builds_meta(stub_replica_configuration):
    REPLICA_NAME=rand_string(10)
    pg=PostgresAdapter()
    pg.load_config(stub_replica_configuration)
    pg._init_image()

    engine=create_engine("postgres://snowshu:snowshu@snowshu_target:9999/snowshu")
    response=engine.execute('SELECT COUNT(*) FROM "snowshu"."snowshu"."replica_meta"')
    assert response.fetchall()[0][0] == 1


