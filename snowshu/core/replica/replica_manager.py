from typing import Optional
from snowshu.core.graph import SnowShuGraph
from snowshu.logger import Logger, duration
from snowshu.core.docker import SnowShuDocker
from snowshu.adapters import target_adapters
from snowshu.core.replica import Replica
logger=Logger().logger

class ReplicaManager:
    """ manages the local replica ecosystem"""
    
    @staticmethod
    def get_replica(name:str,hostname:Optional[str]=None,port:Optional[str]=None)->Replica:
        shdocker=SnowShuDocker()
        image_name=shdocker.sanitize_replica_name(name)
        target_adapter=target_adapters.__dict__[shdocker.get_adapter_name(image_name)]()
        logger.info(f'Mounting target adapter {target_adapter.CLASSNAME}')
        return Replica(image_name,hostname,port,target_adapter)

