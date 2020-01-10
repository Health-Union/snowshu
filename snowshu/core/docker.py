from __future__ import annotations
from typing import Type
import docker
import re
from snowshu.configs import DOCKER_NETWORK,DOCKER_TARGET_CONTAINER,DOCKER_REMOUNT_DIRECTORY
from snowshu.logger import Logger
logger=Logger().logger

class SnowShuDocker:

    def __init__(self):
        self.client=docker.from_env()

    def startup(self,image:str,start_command:str,port:int,envars:list,protocol:str="tcp")->docker.models.containers.Container:
        port_dict={f"{str(port)}/{protocol}":port}
        self.remove_container(DOCKER_TARGET_CONTAINER)
        network=self._get_or_create_network(DOCKER_NETWORK)
        logger.info(f"Creating target container {DOCKER_TARGET_CONTAINER}...")
        logger.info(f"running `{start_command}` against image {image} on network {network.name} for container {DOCKER_TARGET_CONTAINER} with envars {envars}")
        target_container=self.client.containers.run(  image, 
                                            start_command, 
                                            network=network.name,
                                            name=DOCKER_TARGET_CONTAINER,
                                            ports=port_dict, 
                                            environment=envars,
                                            remove=True,
                                            detach=True)
        logger.info(f"Created target container {target_container.name}.")
        return target_container

    def remove_container(self,container:str)->None:
        logger.info(f'Removing existing target container {container}...')
        try:
            removable=self.client.containers.get(container)
            removable.kill()
            logger.info(f'Container {container} removed.')
        except docker.errors.NotFound:
            logger.info(f'existing containers: {[con.name for con in self.client.containers.list(all=True)]}')
            logger.info(f'Container {container} not found, skipping.')
            pass # already removed.

    def _get_or_create_network(self,name:str)->docker.models.networks.Network:
        logger.info(f'Getting docker network {name}...')
        try:
            network=self.client.networks.get(name)
            logger.info(f'Network {network.name} found.')
        except docker.errors.NotFound:
            logger.info(f'Network {name} not found, creating...')
            network=self.client.networks.create(name,check_duplicate=True)
            logger.info(f'Network {network.name} created.')
        return network

    def sanitize_tag_name(self,tag_name:str)->str:
        """
            Much more strict than standard docker tag names.
            Replica names are coerced into ASCII lowercase, dash-seperated a-z0-9 strings when possible.
        """
        logger.info(f'sanitizing tag name {tag_name}...')
        tag='-'.join(re.sub(r'[\-\_\+\.]',' ',tag_name.lower()).split())
        if not re.match(r'^[a-z0-9\-]*$',tag):
            raise ValueError(f'tag name {tag_name} cannot be converted to replica name')
        return tag   
    
    def _remount_replica_data(self,container:docker.models.containers.Container, target_adapter:Type['BaseTargetAdapter'])->bool:
        logger.info('Remounting data inside target...')
        exit_code=container.exec_run(f'cp {target_adapter.NATIVE_DATA_DIRECTORY} /{DOCKER_REMOUNT_DIRECTORY}')[0]
        return exit_code==0
        
