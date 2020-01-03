import pytest
from snowshu.core.models import relation
from snowshu.core.models.materializations import TABLE

def test_single_full_pattern_match():
    test_relation=relation.Relation(database='TEST_DATABASE',schema="TEST_SCHEMA",name="TEST_RELATION",materialization=TABLE, attributes=[])
    pattern=dict(database=".*",schema=".*",name=".*")
    assert relation.single_full_pattern_match(test_relation,pattern)
    
    pattern=dict(database="TEST_DATABASE",schema=".*",name=".*")
    assert relation.single_full_pattern_match(test_relation,pattern)

    pattern=dict(database="(?i)test_.*",schema=".*",name=".*")
    assert relation.single_full_pattern_match(test_relation,pattern)

    pattern=dict(database="(?i)test_.*",schema="TEST_SCHEMA",name=".*")
    assert relation.single_full_pattern_match(test_relation,pattern)

    pattern=dict(database="test_.*",schema="TEST_SCHEMA",name=".*")
    assert not relation.single_full_pattern_match(test_relation,pattern)

def test_at_least_one_full_pattern_match():
    test_relation=relation.Relation(database='TEST_DATABASE',schema="TEST_SCHEMA",name="TEST_RELATION",materialization=TABLE, attributes=[])
    pattern1=dict(database=".*",schema=".*",name=".*")
    pattern2=dict(database="TEST",schema='banana',name='.*')
    pattern3=dict(database="NO",schema='banana',name='.*')

    assert relation.at_least_one_full_pattern_match(test_relation,[pattern1,pattern2,pattern3])
    assert not relation.at_least_one_full_pattern_match(test_relation,[pattern2,pattern3])
