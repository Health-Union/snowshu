import mock
import pytest
from snowshu.core.replica.replica_manager import ReplicaManager
from dataclasses import dataclass
@dataclass
class MockImage:
    tags:list
    labels:dict
    attrs:dict 


mock_response=[
MockImage(['snowshu_replica_snowshu-for_realz'],dict(snowshu_replica='true',source_adapter='snowflake',target_adapter='postgres'),dict(Metadata=dict(LastTagTime='2019-01-1T10:11:01.211101Z'))),
MockImage(['snowshu_replica_also_snowshu'],dict(snowshu_replica='true',source_adapter='big_query',target_adapter='postgres'),dict(Metadata=dict(LastTagTime='2020-06-1T10:12:01.211101Z')))
]

@mock.patch('snowshu.core.replica.replica_manager.SnowShuDocker.find_snowshu_images',return_value=mock_response)
def tests_list_local(docker):
    rep_list=ReplicaManager.list()
    assert 'snowshu-for_realz' in rep_list
