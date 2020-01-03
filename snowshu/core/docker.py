import docker
from snowshu.configs import DOCKER_NETWORK,DOCKER_TARGET_CONTAINER
from snowshu.logger import Logger
logger=Logger().logger

class SnowShuDocker:

    def __init__(self):
        self.client=docker.from_env()

    def startup(self,image:str,start_command:str,port:int,envars:list,protocol:str="tcp")->docker.models.containers.Container:
        port_dict={f"{str(port)}/{protocol}":port}
        ## docker errors here are ambiguous, so we need to explicitly check
        ## and make sure the container does not already exist
        if DOCKER_TARGET_CONTAINER in [container.name for container in self.client.containers.list(all=True)]:
            self.client.containers.get(DOCKER_TARGET_CONTAINER).remove(force=True) #clobber it
        #raise ValueError(str(dict(image=image,start_command=start_command,network=DOCKER_NETWORK,name=DOCKER_TARGET_CONTAINER,ports=port_dict,environment=envars)))
        return self.client.containers.run(  image, 
                                            start_command, 
                                            network=DOCKER_NETWORK,
                                            name=DOCKER_TARGET_CONTAINER,
                                            ports=port_dict, 
                                            environment=envars,
                                            remove=True,
                                            detach=True)
        
   
    def remove_container(self,container:str)->None:
        try:
            self.client.containers.get(container).remove(force=True)   
        except docker.errors.NotFound:
            pass # already removed.

    def _create_network(self,name:str)->docker.models.networks.Network:
        try:
            return self.client.networks.get(name)
        except docker.errors.NotFound:
            return self.client.networks.create(name,check_duplicate=True)
