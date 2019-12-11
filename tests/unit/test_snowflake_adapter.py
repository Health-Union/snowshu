from snowshu.source_adapters.snowflake_adapter import SnowflakeAdapter
from tests.common import rand_string

def test_conn_string_basic():
    sf=SnowflakeAdapter()
    USER,PASSWORD,ACCOUNT=[rand_string(15) for _ in range(3)]
    
    conn_string=sf.get_connection(dict(user=USER,password=PASSWORD,account=ACCOUNT))
    
    assert str(conn_string.url)==f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}'
