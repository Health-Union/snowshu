import pytest
from snowshu.adapters import BaseSQLAdapter
from tests.common import rand_string
from snowshu.core.models.credentials import Credentials, USER,PASSWORD,HOST,ACCOUNT,SCHEMA,DATABASE


def test_sets_credentials():

    base=BaseSQLAdapter()

    base.REQUIRED_CREDENTIALS=(USER,PASSWORD,HOST)
    base.ALLOWED_CREDENTIALS=(ACCOUNT,SCHEMA)
    
    def rand_creds(args)->Credentials:
        kwargs=dict(zip(args,[rand_string(10) for _ in range(len(args))]))
        return Credentials(**kwargs)

    with pytest.raises(KeyError):
        base.credentials=rand_creds((HOST,))
        
    with pytest.raises(KeyError):
        base.credentials=rand_creds((USER,PASSWORD,HOST,DATABASE,))

    base.credentials=rand_creds((USER,PASSWORD,HOST,))
    
    base.credentials=rand_creds((USER,PASSWORD,HOST,ACCOUNT,))
