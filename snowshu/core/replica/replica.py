from typing import Type
from snowshu.configs import IS_IN_DOCKER
from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.core.docker import SnowShuDocker


class Replica:
    """The actual live container instance of the replica."""

    def __init__(self,
                 image: str,
                 port: int,
                 target_adapter: Type[BaseTargetAdapter]):
        shdocker = SnowShuDocker()
        self.name, self.port = shdocker.replica_image_name_to_common_name(
            image), port
        self.container = shdocker.get_stopped_container(
            image,
            target_adapter.DOCKER_REPLICA_START_COMMAND,
            target_adapter.DOCKER_REPLICA_ENVARS,
            port)

    def launch(self) -> None:
        self.container.start()
        message = f"""

Replica {self.name} has been launched and started.
To stop your replica temporarily, use command `snowshu stop {self.name}`.
To spin down your replica, use command `snowshu down {self.name}`.

You can connect directly from your host computer using the connection string

snowshu:snowshu@localhost:{self.port}/snowshu

"""
        if IS_IN_DOCKER:
            message += f"""You can connect to the sample database from within docker containers running on the `snowshu` docker network.
use the connection string

snowshu:snowshu@{self.name}:{self.port}/snowshu

to connect."""
        return message
