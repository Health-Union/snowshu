import pytest

from snowshu.core.utils import case_insensitive_dict_value, correct_case


def test_case_insensitive_search():

    camelcased=dict(cOlUmn1=1, ColumN2=2)

    assert case_insensitive_dict_value(camelcased,'column1') == 1

def test_correct_case():

    correct = ['upper','LOWER','SNAKE_CASED','lower_snake','_UNDERSCORE_START','_lower_underscore_start']
    leave = ['Upper','lOWER','Space Cased','^regex$','Other_snake','UNIFORM SPACE CASED','uniform lower space',\
             '0248_num_start','0248_NUM_START','0248','0','$dollar_start']

    def correct_test_suite(under_test,upper):
        for item in under_test:
            expected = item.upper() if upper else item.lower()
            assert correct_case(item,upper) == expected

    def leave_test_suite(under_test,upper):
        for item in under_test:
            assert correct_case(item,upper) == item

    [correct_test_suite(correct,x) for x in (True,False,)]
    [leave_test_suite(leave,x) for x in (True,False,)]
