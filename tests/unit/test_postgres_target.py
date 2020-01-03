import pytest
import mock
from sqlalchemy import create_engine
from tests.common import rand_string
from snowshu.adapters.target_adapters import PostgresAdapter

@pytest.mark.skip
@mock.patch('snowshu.adapters.base_sql_adapter.sqlalchemy')
def test_spins_up_container(sqlalchemy,stub_replica_configuration):
    REPLICA_NAME=rand_string(10)
    pg=PostgresAdapter()
    pg.load_config(stub_replica_configuration)
    mocked_sqlalchemy=mock.MagicMock()
    pg.get_connection=lambda : mocked_sqlalchemy
      
    pg._init_image()
    for attr in ('user','password','database'):
        assert pg._credentials.__dict__[attr] == 'snowshu'
    assert pg._credentials.host == 'snowshu_target'    
    assert pg._credentials.port == 9999

    assert mocked_sqlalchemy.execute.called_with('CREATE DATABASE snowshu; CREATE SCHEMA "snowshu"."snowshu"')

def test_builds_meta(stub_replica_configuration):
    REPLICA_NAME=rand_string(10)
    pg=PostgresAdapter()
    pg.load_config(stub_replica_configuration)
    pg._init_image()

    engine=create_engine("postgres://snowshu:snowshu@snowshu_target:9999/snowshu")
    response=engine.execute('SELECT COUNT(*) FROM "snowshu"."snowshu"."replica_meta"')
    assert response.fetchall()[0][0] == 1


