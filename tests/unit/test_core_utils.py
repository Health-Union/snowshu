import pytest
from snowshu.core.utils import case_insensitive_dict_value

def test_case_insensitive_search():

    camelcased=dict(cOlUmn1=1, ColumN2=2)

    assert case_insensitive_dict_value(camelcased,'column1') == 1
