import pytest
import mock
from tests.common import rand_string
import pandas as pd
from snowshu.source_adapters.base_source_adapter import BaseSourceAdapter


def test_get_databases():
    base=BaseSourceAdapter()
    base.GET_ALL_DATABASES_SQL='this should be sql'
    db_response=mock.MagicMock()
    response_frame=pd.DataFrame([dict(database_name=rand_string(10)) for _ in range(10)])
    db_response.return_value=response_frame
    base._safe_query=db_response
    
    assert base.get_all_databases()[0] == response_frame.iloc[0]['database_name']
    assert base.get_all_databases()[9] == response_frame.iloc[9]['database_name']

