from __future__ import annotations

import re
from typing import TYPE_CHECKING, List, Optional, Type, Dict
import logging

import docker

from snowshu.configs import (DOCKER_NETWORK, DOCKER_TARGET_CONTAINER, DOCKER_REPLICA_MOUNT_FOLDER,
                             DOCKER_WORKING_DIR, DOCKER_REPLICA_VOLUME, LOCAL_ARCHITECTURE, POSTGRES_IMAGE)

if TYPE_CHECKING:
    from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter

logger = logging.getLogger(__name__)


class SnowShuDocker:

    def __init__(self, target_arch: list = None):
        self.client = docker.from_env()
        self.target_arch = target_arch

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

            container_arch = container.attrs['Config']['Image'].split(':')[1]

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

    # TODO: this is all holdover from storages, and can be greatly simplified.
    def get_stopped_container(  # noqa pylint: disable=too-many-arguments
            self,
            image_name,
            is_incremental: bool,
            start_command: str,
            envars: list,
            port: int,
            target_adapter: Type['BaseTargetAdapter'],
            name: Optional[str] = None,
            labels: dict = None,
            protocol: str = "tcp") -> tuple(docker.models.containers.Container):
        if not labels:
            labels = {}
        name = name if name else self.replica_image_name_to_common_name(image_name)
        port_dict = {f"{str(port)}/{protocol}": port}

        self.remove_container(name)
        network = self._get_or_create_network(DOCKER_NETWORK)

        logger.info('Creating an external volume...')
        replica_volume = self._create_snowshu_volume(DOCKER_REPLICA_VOLUME)

        logger.info(f'Finding base image {image_name}...')
        container_list = []

        if not self.target_arch:
            arch_list = [LOCAL_ARCHITECTURE]
        else:
            arch_list = self.target_arch

        # In case non-native replica is supplied as a base for incremental build,
        # we need to override most optimal build order, so that there is logical continuity for the user
        if is_incremental:
            try:
                supplied_image_arch = self.client.images.get(image_name).attrs['Architecture']
                if arch_list[0] != supplied_image_arch and len(arch_list) == 1:
                    # In the case of building just a single arch, replace native (default) with supplied
                    arch_list[0] = supplied_image_arch
                elif arch_list[0] != supplied_image_arch and len(arch_list) == 2:
                    # In the case of multiarch build, reverse the list
                    arch_list.reverse()
                else:
                    # Keep the list untouched if supplied base replica is of a native arch
                    pass
            except docker.errors.ImageNotFound:
                logger.exception(
                    f'Supplied incremental base image {image_name} not found locally, aborting build')
                raise
        
        for arch in arch_list:
            try:
                if is_incremental:
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
                    else:
                        # If supplied image is not of current arch, pull postgres instead
                        logger.info(
                            f'Base image is NOT of target arch {arch}, using postgres instead...')

                        try:
                            image = self.client.images.get(
                                f'{target_adapter.DOCKER_IMAGE.split(":")[0]}:{arch}')  # noqa pylint: disable=use-maxsplit-arg
                        except docker.errors.ImageNotFound:
                            image = self.client.images.pull(
                                target_adapter.DOCKER_IMAGE, platform=f'linux/{arch}')
                            image.tag(f'{target_adapter.DOCKER_IMAGE.split(":")[0]}:{arch}')  # noqa pylint: disable=use-maxsplit-arg

                else:
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

            tagged_container_name = f'{name}_{arch}'
            logger.info(f"Creating stopped container {tagged_container_name}...")
            self.remove_container(tagged_container_name)
            container = self.client.containers.create(f'{image_name.split(":")[0]}:{arch}',
                                                      start_command,
                                                      network=network.name,
                                                      name=tagged_container_name,
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
                is_incremental: bool,
                start_command: str,
                port: int,
                target_adapter: Type['BaseTargetAdapter'],
                source_adapter: str,
                envars: list,
                protocol: str = "tcp") -> tuple(docker.models.containers.Container):  # noqa pylint: disable=unused-argument

        active_container, passive_container = self.get_stopped_container(
            image_name=image,
            is_incremental=is_incremental,
            start_command=start_command,
            envars=envars,
            port=port,
            name=DOCKER_TARGET_CONTAINER,
            target_adapter=target_adapter,
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

            try:
                container.start()
            except docker.errors.APIError as error:
                if 'port is already allocated' in error.explanation:
                    logger.exception('One of the ports used by snowshu_target is '
                                     'already allocated, stop extra containers and rerun')
                raise

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
