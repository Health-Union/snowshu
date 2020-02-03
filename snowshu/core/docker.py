from __future__ import annotations
from typing import Type, Optional,List
import docker
import re
from snowshu.configs import DOCKER_NETWORK,\
    DOCKER_TARGET_CONTAINER
from snowshu.logger import Logger
logger = Logger().logger


class SnowShuDocker:

    def __init__(self):
        self.client = docker.from_env()

    def convert_container_to_replica(
            self,
            replica_name: str,
            container: docker.models.containers.Container,
            target_adapter: Type['BaseTargetAdapter']) -> docker.models.images.Image:
        """coerces a live container into a replica image and returns the image.

        replica_name: the name of the new replica
        """
        self._remount_replica_data(container, target_adapter)
        replica_name = self.sanitize_replica_name(replica_name)
        logger.info(f'Creating new replica image with name {replica_name}...')
        try:
            self.client.images.remove(replica_name, force=True)
        except docker.errors.ImageNotFound:
            pass
        replica = container.commit(
            repository=self.sanitize_replica_name(replica_name),
            changes=target_adapter.docker_commit_changes()
            )
        logger.info(f'Replica image {replica.tags[0]} created. Cleaning up...')
        self.remove_container(container.name)

        return replica
    ## TODO: this is all holdover from storages, and can be greatly simplified.
    def get_stopped_container(
            self,
            image,
            start_command: str,
            envars: list,
            port: int,
            name: Optional[str] = None,
            labels: dict = dict(),
            protocol: str = "tcp") -> docker.models.containers.Container:
        name = name if name else self.replica_image_name_to_common_name(image)
        logger.info(f'Finding base image {image}...')
        try:
            self.client.images.get(image)
        except docker.errors.ImageNotFound:
            parsed_image = image.split(':')
            if len(parsed_image) > 1:
                self.client.images.pull(parsed_image[0], tag=parsed_image[1])
            else:
                self.client.images.pull(parsed_image[0])

        port_dict = {f"{str(port)}/{protocol}": port}

        self.remove_container(name)
        network = self._get_or_create_network(DOCKER_NETWORK)
        logger.info(f"Creating stopped container {name}...")
        container = self.client.containers.create(image,
                                                  start_command,
                                                  network=network.name,
                                                  name=name,
                                                  hostname=name,
                                                  ports=port_dict,
                                                  environment=envars,
                                                  labels=labels,
                                                  detach=True)
        logger.info(f"Created stopped container {container.name}.")
        return container

    def startup(self,
                image: str,
                start_command: str,
                port: int,
                target_adapter: str,
                source_adapter: str,
                envars: list,
                protocol: str = "tcp") -> docker.models.containers.Container:

        container = self.get_stopped_container(
            image,
            start_command,
            envars,
            port,
            name=DOCKER_TARGET_CONTAINER,
            labels=dict(
                snowshu_replica='true',
                target_adapter=target_adapter,
                source_adapter=source_adapter))
        logger.info(
            f'Connecting {DOCKER_TARGET_CONTAINER} to bridge network..')
        self._connect_to_bridge_network(container)
        logger.info(
            f'Connected. Starting created container {DOCKER_TARGET_CONTAINER}...')
        container.start()
        logger.info(f'Container {DOCKER_TARGET_CONTAINER} started.')
        return container

    def remove_container(self, container: str) -> None:
        logger.info(f'Removing existing target container {container}...')
        try:
            removable = self.client.containers.get(container)
            try:
                removable.kill()
            except docker.errors.APIError:
                logger.info(f'Container {container} already stopped.')
                pass
            removable.remove()
            logger.info(f'Container {container} removed.')
        except docker.errors.NotFound:
            logger.info(f'Container {container} not found, skipping.')
            pass  # already removed.

    def _get_or_create_network(
            self, name: str) -> docker.models.networks.Network:
        logger.info(f'Getting docker network {name}...')
        try:
            network = self.client.networks.get(name)
            logger.info(f'Network {network.name} found.')
        except docker.errors.NotFound:
            logger.info(f'Network {name} not found, creating...')
            network = self.client.networks.create(name, check_duplicate=True)
            logger.info(f'Network {network.name} created.')
        return network

    def _connect_to_bridge_network(
            self, container: docker.models.containers.Container) -> None:
        logger.info('Adding container to bridge...')
        bridge = self.client.networks.get('bridge')
        bridge.connect(container)
        logger.info(f'Connected container {container.name} to bridge network.')

    def get_adapter_name(self, name: str) -> str:
        try:
            return self.client.images.get(name).labels['target_adapter']
        except KeyError:
            message = "Replica image {name} is corrupted; no label for `target_adapter`."
            logger.critical(message)
            raise AttributeError(message)

    def sanitize_replica_name(self, name: str) -> str:
        """Much more strict than standard docker tag names.

        ReplicaFactory names are coerced into ASCII lowercase, dash-
        seperated a-z0-9 strings when possible.
        """
        prefix = "snowshu_replica_"
        image = '-'.join(re.sub(r'[\-\_\+\.]', ' ',
                                name.lower().replace(prefix, '')).split())
        if not re.fullmatch(r'^[a-z0-9\-]*$', image):
            raise ValueError(
                f'Replica name {name} cannot be converted to replica name')
        final_image = prefix + image
        return final_image

    def replica_image_name_to_common_name(self, name: str) -> str:
        """reverse the replica sanitizer."""
        sr='snowshu_replica_'
        return ':'.join((sr.join(name.split(sr)[1:])).split(':')[:-1])

    def _remount_replica_data(
            self,
            container: docker.models.containers.Container,
            target_adapter: Type['BaseTargetAdapter']) -> None:
        logger.info('Remounting data inside target...')
        for command in target_adapter.image_finalize_bash_commands():
            response = container.exec_run(f"/bin/bash -c '{command}'", tty=True)
            if response[0] > 0:
                raise OSError(response[1])
        logger.info('Data remounted, image ready to be finalized.')

    def find_snowshu_images(self)->List[docker.models.images.Image]:
        return [img for img in filter((lambda x : len(x.tags) > 0),self.client.images.list(filters=dict(label='snowshu_replica=true')))]
