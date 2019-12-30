import pytest
from sqlalchemy import create_engine
import testing.postgresql
from snowshu.adapters.target_adapters import PostgresAdapter

@pytest.mark.skip
def test_postgres_create_table(stub_relation):
    adapter=PostgresAdapter()

    with testing.postgresql.Postgresql() as pgsql:
        adapter._build_conn_string=pgsql.url()
        
        assert adapter.create_relation(stub_relation)
    
        check_query=f"""SELECT * FROM "{relation.database}"."INFORMATION_SCHEMA"."TABLES" WHERE table_name = '{relation.name}'"""
        engine=create_engine(pgsql.url())
        result = engine.execute(check_query).fetchall()
        assert len(result) == 1
