import pytest
import mock
from sqlalchemy import create_engine
from tests.common import rand_string
from snowshu.adapters.target_adapters import PostgresAdapter


@mock.patch('snowshu.adapters.base_sql_adapter.sqlalchemy')
def test_spins_up_container(sqlalchemy,stub_replica_configuration):
    REPLICA_NAME=rand_string(10)
    base=PostgresAdapter()
    base.load_config(stub_replica_configuration)
    mocked_sqlalchemy=mock.MagicMock()
    base.get_connection=lambda : mocked_sqlalchemy
    
    base._init_image()
    for attr in ('user','password','database'):
        assert base._credentials.__dict__[attr] == 'snowshu'
    assert base._credentials.host == 'snowshu_target'    
    assert base._credentials.port == 9999

    assert mocked_sqlalchemy.execute.called_with('CREATE DATABASE snowshu; CREATE SCHEMA "snowshu"."snowshu"')
    ## creates image
    ## all creds set to "snowshu"
    ## creates snowshu DB
    

def test_builds_meta(stub_replica_configuration):
    REPLICA_NAME=rand_string(10)
    base=PostgresAdapter()
    base.load_config(stub_replica_configuration)
    base._init_image()

    engine=create_engine("postgres://snowshu:snowshu@snowshu_target/snowshu")
    response=engine.execute('SELECT COUNT(*) FROM "snowshu"."snowshu"."replica_meta"')
    assert response.fetchall()[0] == 1


