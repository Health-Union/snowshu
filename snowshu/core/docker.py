from __future__ import annotations

import re
from typing import TYPE_CHECKING, List, Type, Dict
import logging

import docker

from snowshu.configs import (DOCKER_NETWORK, DOCKER_REPLICA_MOUNT_FOLDER,
                             DOCKER_WORKING_DIR, DOCKER_REPLICA_VOLUME, LOCAL_ARCHITECTURE)

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
            volume = self.client.volumes.create(
                name=volume_name, driver='local',)
        return volume

    def convert_container_to_replica(
            self,
            replica_name: str,
            active_container: docker.models.containers.Container,
            passive_container: docker.models.containers.Container) -> list[docker.models.images.Image]:
        """coerces a live container into a replica image and returns the image.

        replica_name: the name of the new replica

        return: [replica_image_from_active,
                 replica_image_from_passive(skipped if no passive),
                 replica_image_from_local_arch]
        """
        new_replica_name = self.sanitize_replica_name(replica_name)
        replica_list = []
        container_list = [
            active_container, passive_container] if passive_container else [active_container]

        logger.info(
            f'Creating new replica image with name {new_replica_name}...')

        for container in container_list:
            try:
                self.client.images.remove(new_replica_name, force=True)
            except docker.errors.ImageNotFound:
                pass

            container_arch = container.name.split('_')[-1]

            # commit with arch tag
            replica = container.commit(
                repository=new_replica_name, tag=container_arch)
            replica_list.append(replica)

            logger.info(
                f'Replica image {replica.tags[0]} created. Cleaning up...')
            self.remove_container(container.name)

        for replica in replica_list:
            if replica.attrs.get('Architecture') == LOCAL_ARCHITECTURE:
                local_arch_replica = replica
                local_arch_replica.tag(
                    repository=new_replica_name, tag='latest')

        # this is done due to how recomitting existing image is not reflected in 'replica_list' var
        actual_replica_list = self.client.images.list(new_replica_name)

        return actual_replica_list

    def startup(self,  # noqa pylint: disable=too-many-arguments
                              target_adapter: Type['BaseTargetAdapter'],
                              source_adapter: str,
                              arch_list: list[str],
                              envars: list) -> tuple(docker.models.containers.Container):

        # Unpack target adapter's data
        image_name = target_adapter.DOCKER_IMAGE
        is_incremental = target_adapter.is_incremental
        hostname = target_adapter.credentials.host

        network = self._get_or_create_network(DOCKER_NETWORK)

        logger.info('Creating an external volume...')
        replica_volume = self._create_snowshu_volume(DOCKER_REPLICA_VOLUME)

        logger.info(f'Finding base image {image_name}...')
        container_list = []

        if is_incremental:
            name = self.replica_image_name_to_common_name(image_name)
            for arch in arch_list:
                try:
                    # Try to retreive supplied image
                    try:
                        image_candidate = self.client.images.get(image_name)
                    except docker.errors.ImageNotFound:
                        logger.exception(
                            f'Supplied incremental base image {image_name} not found locally, aborting build')
                        raise

                    # Check supplied image's arch, if local, pass it further
                    if image_candidate.attrs['Architecture'] == arch:
                        logger.info(
                            f'Base image is of target arch {arch}, using it...')
                        image = image_candidate
                    elif len(arch_list) == 1:
                        # If the build is not multiarch, we should still go through with it
                        logger.warning(
                            f'Base image is NOT of target arch {arch}, but only one arch was requested, continuing...')
                        image = image_candidate
                    else:
                        # If supplied image is not of current arch, pull postgres instead
                        logger.info(
                            f'Base image is NOT of target arch {arch}, using base db image instead...')
                        try:
                            image = self.client.images.get(
                                f'{target_adapter.BASE_DB_IMAGE.split(":")[0]}:{arch}')  # noqa pylint: disable=use-maxsplit-arg
                        except docker.errors.ImageNotFound:
                            image = self.client.images.pull(
                                target_adapter.BASE_DB_IMAGE, platform=f'linux/{arch}')
                            image.tag(f'{target_adapter.BASE_DB_IMAGE.split(":")[0]}:{arch}')  # noqa pylint: disable=use-maxsplit-arg

                except ConnectionError as error:
                    logger.error(
                        'Looks like docker is not started, please start docker daemon\nError: %s', error)
                    raise

                tagged_container_name = f'{name}_{arch}'
                logger.info(
                    f"Creating stopped container {tagged_container_name}...")
                self.remove_container(tagged_container_name)

                container = self.create_and_init_container(
                    image=image,
                    container_name=tagged_container_name,
                    target_adapter=target_adapter,
                    source_adapter=source_adapter,
                    network=network,
                    replica_volume=replica_volume,
                    envars=envars
                )

                if len(arch_list) > 1:
                    container.stop()
                container_list.append(container)
        else:
            for arch in arch_list:
                try:
                    # This pulls raw postgres for regular full build
                    try:
                        image = self.client.images.get(
                            f'{target_adapter.DOCKER_IMAGE.split(":")[0]}:{arch}')  # noqa pylint: disable=use-maxsplit-arg
                    except docker.errors.ImageNotFound:
                        image = self.client.images.pull(
                            target_adapter.DOCKER_IMAGE, platform=f'linux/{arch}')
                        image.tag(f'{target_adapter.DOCKER_IMAGE.split(":")[0]}:{arch}')  # noqa pylint: disable=use-maxsplit-arg

                    # verify the image is tagged properly (image's arch matches its tag)
                    try:
                        assert image.attrs['Architecture'] == arch
                    except AssertionError:
                        logger.warning('Image tags do not match their actual architecture, '
                                       'retag or delete postgres images manually to correct')

                except ConnectionError as error:
                    logger.error(
                        'Looks like docker is not started, please start docker daemon\nError: %s', error)
                    raise

                tagged_container_name = f'{hostname}_{arch}'
                logger.info(
                    f"Creating stopped container {tagged_container_name}...")
                self.remove_container(tagged_container_name)

                container = self.create_and_init_container(
                    image=image,
                    container_name=tagged_container_name,
                    target_adapter=target_adapter,
                    source_adapter=source_adapter,
                    network=network,
                    replica_volume=replica_volume,
                    envars=envars
                )

                if len(arch_list) > 1:
                    container.stop()
                container_list.append(container)

        if len(container_list) == 2:
            active_container, passive_container = container_list[0], container_list[1]
        else:
            active_container = container_list[0]
            passive_container = None

        if len(arch_list) > 1:
            active_container.start()

        return active_container, passive_container

    def create_and_init_container(  # noqa pylint: disable=too-many-arguments
                                     self,
                                     image: docker.models.images.Image,
                                     container_name: str,
                                     target_adapter: Type['BaseTargetAdapter'],
                                     source_adapter: str,
                                     network: docker.models.networks.Network,
                                     replica_volume: docker.models.volumes.Volume,
                                     envars: dict
                                 ) -> docker.models.containers.Container:
        """ Method used during self.startup() execution, creates, starts and setups container
            
            input: some stuff needed to define a container launch
            return: container object instance, in a running state and already set up
        """

        logger.info(
            f"Creating stopped container {container_name}...")

        port = target_adapter.DOCKER_TARGET_PORT
        hostname = target_adapter.credentials.host
        protocol = 'tcp'
        port_dict = {f"{str(port)}/{protocol}": port}

        self.remove_container(container_name)

        container = self.client.containers.create(
            image.tags[0],
            target_adapter.DOCKER_START_COMMAND,
            network=network.name,
            name=container_name,
            hostname=hostname,
            ports=port_dict,
            environment=envars,
            labels=dict(
                snowshu_replica='true',
                target_adapter=target_adapter.CLASSNAME,
                source_adapter=source_adapter),
            detach=True,
            volumes={replica_volume.name: {
                'bind': f'{DOCKER_REPLICA_MOUNT_FOLDER}'
            }},
            working_dir=DOCKER_WORKING_DIR
        )
        logger.info(
            f"Created stopped container {container.name}, connecting it to bridge network...")
        self._connect_to_bridge_network(container)
        logger.info(
            f'Connected. Starting created container {container.name}...')
        try:
            container.start()
        except docker.errors.APIError as error:
            if 'port is already allocated' in error.explanation:
                logger.exception('One of the ports used by snowshu_target is '
                                 'already allocated, stop extra containers and rerun')
            raise
        logger.info(
            f'Container {container.name} started, running initial setup...')
        self._run_container_setup(container, target_adapter)
        logger.info(f'Container {container.name} fully initialized.')

        return container

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
            response = container.exec_run(
                f"/bin/bash -c '{command}'", tty=True)
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
