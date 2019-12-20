import pytest
import snowshu.core.utils as utils
from snowshu.core.models import Relation
from snowshu.core.models.materializations import TABLE

def test_single_full_pattern_match():
    relation=Relation(database='TEST_DATABASE',schema="TEST_SCHEMA",name="TEST_RELATION",materialization=TABLE, attributes=[])
    pattern=dict(database=".*",schema=".*",name=".*")
    assert utils.single_full_pattern_match(relation,pattern)
    
    pattern=dict(database="TEST_DATABASE",schema=".*",name=".*")
    assert utils.single_full_pattern_match(relation,pattern)

    pattern=dict(database="(?i)test_.*",schema=".*",name=".*")
    assert utils.single_full_pattern_match(relation,pattern)

    pattern=dict(database="(?i)test_.*",schema="TEST_SCHEMA",name=".*")
    assert utils.single_full_pattern_match(relation,pattern)

    pattern=dict(database="test_.*",schema="TEST_SCHEMA",name=".*")
    assert not utils.single_full_pattern_match(relation,pattern)

def test_at_least_one_full_pattern_match():
    relation=Relation(database='TEST_DATABASE',schema="TEST_SCHEMA",name="TEST_RELATION",materialization=TABLE, attributes=[])
    pattern1=dict(database=".*",schema=".*",name=".*")
    pattern2=dict(database="TEST",schema='banana',name='.*')
    pattern3=dict(database="NO",schema='banana',name='.*')

    assert utils.at_least_one_full_pattern_match(relation,[pattern1,pattern2,pattern3])
    assert not utils.at_least_one_full_pattern_match(relation,[pattern2,pattern3])
