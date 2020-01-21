import pytest
from snowshu.adapters import BaseSQLAdapter
from tests.common import rand_string
from snowshu.core.models.credentials import Credentials, USER, PASSWORD, HOST, ACCOUNT, SCHEMA, DATABASE, ROLE


def rand_creds(args) -> Credentials:
    kwargs = dict(zip(args, [rand_string(10) for _ in range(len(args))]))
    return Credentials(**kwargs)


class TestAdapter(BaseSQLAdapter):

    REQUIRED_CREDENTIALS = (USER, PASSWORD, HOST)
    ALLOWED_CREDENTIALS = (ACCOUNT, SCHEMA)
    DATA_TYPE_MAPPINGS = dict()
    MATERIALIZATION_MAPPINGS = dict()


def test_sets_credentials():

    base = TestAdapter()

    with pytest.raises(KeyError):
        base.credentials = rand_creds((HOST,))

    with pytest.raises(KeyError):
        base.credentials = rand_creds((USER, PASSWORD, HOST, DATABASE,))

    base.credentials = rand_creds((USER, PASSWORD, HOST,))

    base.credentials = rand_creds((USER, PASSWORD, HOST, ACCOUNT,))


def test_default_conn_string():
    base = TestAdapter()
    base.dialect = 'postgres'

    base.REQUIRED_CREDENTIALS = (USER, PASSWORD, DATABASE, HOST)
    base.ALLOWED_CREDENTIALS = (ROLE, SCHEMA, ACCOUNT)

    creds = rand_creds((USER, PASSWORD, HOST, DATABASE, ACCOUNT,))
    base.credentials = creds

    assert base._build_conn_string(
    ) == f'postgres://{creds.user}:{creds.password}@{creds.host}/{creds.database}?account={creds.account}'
