from snowshu.source_adapters.snowflake_adapter import SnowflakeAdapter
from tests.common import rand_string
from snowshu.core.credentials import Credentials

def test_conn_string_basic():
    sf=SnowflakeAdapter()
    USER,PASSWORD,ACCOUNT,DATABASE=[rand_string(15) for _ in range(4)]
    
    creds=Credentials(user=USER,password=PASSWORD,account=ACCOUNT,database=DATABASE)

    sf.credentials=creds
    
    conn_string=sf.get_connection()
    
    assert str(conn_string.url)==f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/{DATABASE}/'
