from dataclasses import dataclass
from tests.common import random_date
from datetime import datetime

@dataclass
class MockImage:
    tags:list
    labels:dict
    attrs:dict 

class MockImageFactory:
    
    @staticmethod
    def get_image(
                 name:str,
                 is_snowshu:bool=True,
                 source_adapter:str='snowflake',
                 target_adapter:str='postgres')->MockImage:

        labels=dict()
        if is_snowshu:
            name='snowshu_replica_'+name
            labels=dict(snowshu_replica='true',source_adapter=source_adapter,target_adapter=target_adapter)
        
        return MockImage([name],
                         labels,
                         dict(Metadata=dict(LastTagTime=datetime.strftime(random_date(),'%Y-%m-%dT%H:%M:%S.%fZ'))))
                         

