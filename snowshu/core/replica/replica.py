from typing import Type
from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.core.docker import SnowShuDocker

class Replica:
    """ The actual live container instance of the replica"""
    def __init__(self,
                 image:str,
                 target_adapter:Type[BaseTargetAdapter]):
        shdocker=SnowShuDocker()
        self.name=image
        self.container=shdocker.launch(
                                        image,
                                        target_adapter.DOCKER_REPLICA_START_COMMAND,
                                        target_adapter.DOCKER_REPLICA_ENVARS,
                                        port=target_adapter.DOCKER_TARGET_PORT)


