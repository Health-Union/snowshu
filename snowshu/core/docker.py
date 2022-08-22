from __future__ import annotations

import re
from typing import TYPE_CHECKING, List, Optional, Type, Dict
import logging

import docker

from snowshu.configs import (DOCKER_NETWORK, DOCKER_TARGET_CONTAINER, DOCKER_REPLICA_MOUNT_FOLDER,
                             DOCKER_WORKING_DIR, DOCKER_REPLICA_VOLUME, LOCAL_ARCHITECTURE, TARGET_ARCHITECTURE)

if TYPE_CHECKING:
    from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter

logger = logging.getLogger(__name__)


class SnowShuDocker:

    def __init__(self):
        self.client = docker.from_env()

    def _create_snowshu_volume(self, volume_name: str) -> docker.models.volumes.Volume:
        """ Creating a docker volume if not exists"""
        try:
            volume = self.client.volumes.get(volume_name)
        except docker.errors.NotFound:
            volume = self.client.volumes.create(name=volume_name, driver='local',)
        return volume

    def convert_container_to_replica(
            self,
            replica_name: str,
            active_container: docker.models.containers.Container,
            passive_container: docker.models.containers.Container) -> None:
        """coerces a live container into a replica image and returns the image.

        replica_name: the name of the new replica
        """
        container_list = [
            active_container, passive_container] if passive_container else [active_container]
        for container in container_list:
            container.start()
            new_replica_name = f"{self.sanitize_replica_name(replica_name)}_{container.name.replace('snowshu_target_', '')}"  # noqa pycodestyle: disable=line-too-long
            logger.info(
                f'Creating new replica image with name {new_replica_name}...')
            try:
                self.client.images.remove(new_replica_name, force=True)
            except docker.errors.ImageNotFound:
                pass
            replica = container.commit(
                repository=replica_name, tag=container.name.replace('snowshu_target_', ''))
            logger.info(
                f'Replica image {replica.tags[0]} created. Cleaning up...')
            self.remove_container(container.name)

    # TODO: this is all holdover from storages, and can be greatly simplified.
    def get_stopped_container(  # noqa pylint: disable=too-many-arguments
            self,
            image,
            start_command: str,
            envars: list,
            port: int,
            name: Optional[str] = None,
            labels: dict = None,
            protocol: str = "tcp") -> tuple(docker.models.containers.Container):
        if not labels:
            labels = {}
        name = name if name else self.replica_image_name_to_common_name(image)
        port_dict = {f"{str(port)}/{protocol}": port}

        self.remove_container(name)
        network = self._get_or_create_network(DOCKER_NETWORK)

        logger.info('Creating an external volume...')
        replica_volume = self._create_snowshu_volume(DOCKER_REPLICA_VOLUME)

        if not TARGET_ARCHITECTURE:
            arch_list = [LOCAL_ARCHITECTURE]
        else:
            arch_list = TARGET_ARCHITECTURE

        logger.info(f'Finding base image {image}...')
        container_list = []
        for arch in arch_list:
            try:
                new_image = self.client.images.pull(
                    image, platform=f'linux/{arch}')
                new_image.tag(f'{image.split(":")[0]}:{arch}')
            except ConnectionError as error:
                logger.error(
                    'Looks like docker is not started, please start docker daemon\nError: %s', error)
                raise

            tagged_name = f'{name}_{arch}'
            logger.info(f"Creating stopped container {name}...")
            self.remove_container(tagged_name)
            container = self.client.containers.create(f'{image.split(":")[0]}:{arch}',
                                                      start_command,
                                                      network=network.name,
                                                      name=tagged_name,
                                                      hostname=name,
                                                      ports=port_dict,
                                                      environment=envars,
                                                      labels=labels,
                                                      detach=True,
                                                      volumes={replica_volume.name: {
                                                          'bind': f'{DOCKER_REPLICA_MOUNT_FOLDER}',
                                                          # Make sure passive container does not mess up common volume
                                                          'mode': 'rw' if arch == arch_list[0] else 'ro'
                                                      }},
                                                      working_dir=DOCKER_WORKING_DIR
                                                      )
            logger.info(f"Created stopped container {container.name}.")
            container_list.append(container)

        # downstream code expects 2 containers, active and passive / active and None (if running only one at a time)
        if len(container_list) != 2:
            container_list.append(None)

        return container_list[0], container_list[1]

    def startup(self,  # noqa pylint: disable=too-many-arguments
                image: str,
                start_command: str,
                port: int,
                target_adapter: Type['BaseTargetAdapter'],
                source_adapter: str,
                envars: list,
                protocol: str = "tcp") -> tuple(docker.models.containers.Container):  # noqa pylint: disable=unused-argument

        active_container, passive_container = self.get_stopped_container(
            image,
            start_command,
            envars,
            port,
            name=DOCKER_TARGET_CONTAINER,
            labels=dict(
                snowshu_replica='true',
                target_adapter=target_adapter.CLASSNAME,
                source_adapter=source_adapter))

        container_list = [active_container, passive_container] if passive_container else [
            active_container]
        # have to do the dance with start/stop due to both containers using same ports
        for container in container_list:
            logger.info(
                f'Connecting {container.name} to bridge network..')
            self._connect_to_bridge_network(container)
            logger.info(
                f'Connected. Starting created container {container.name}...')
            container.start()
            logger.info(f'Container {container.name} started.')
            logger.info(f'Running initial setup on {container.name}...')
            self._run_container_setup(container, target_adapter)
            logger.info(f'Container {container.name} fully initialized.')

            if len(container_list) > 1:
                container.stop()

        if len(container_list) > 1:
            active_container.start()
        return active_container, passive_container

    def remove_container(self, container: str) -> None:
        logger.info(f'Removing existing target container {container}...')
        try:
            removable = self.client.containers.get(container)
            try:
                removable.kill()
            except docker.errors.APIError:
                logger.info(f'Container {container} already stopped.')

            removable.remove()
            logger.info(f'Container {container} removed.')
        except docker.errors.NotFound:
            logger.info(f'Container {container} not found, skipping.')

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
        except KeyError as exc:
            message = "Replica image {name} is corrupted; no label for `target_adapter`."
            logger.critical(message)
            raise AttributeError(message) from exc

    @staticmethod
    def sanitize_replica_name(name: str) -> str:
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

    @staticmethod
    def replica_image_name_to_common_name(name: str) -> str:
        """reverse the replica sanitizer."""
        sr_delimeter = 'snowshu_replica_'
        return ':'.join((sr_delimeter.join(name.split(sr_delimeter)[1:])).split(':')[:-1])

    @staticmethod
    def _run_container_setup(container: docker.models.containers.Container,
                             target_adapter: Type['BaseTargetAdapter']) -> None:
        logger.info('Running initialization commands in container...')
        for command in target_adapter.image_initialize_bash_commands():
            response = container.exec_run(f"/bin/bash -c '{command}'", tty=True)
            if response[0] > 0:
                raise OSError(response[1])
        logger.info('Setup commands finished.')

    def find_snowshu_images(self) -> List[docker.models.images.Image]:
        return list(filter((lambda x: len(x.tags) > 0), self.client.images.list(
            filters=dict(label='snowshu_replica=true'))))

    def get_docker_image_attributes(self, image: str) -> Dict:
        """
            Retrieve image-related attributes
        """
        return self.client.images.get(image).attrs
