import docker

class SnowShuDocker:

    def __init__(self):
        self.client=docker.from_env()

    @property
    def container(self)->docker.models.containers.Container:
        return self._container

    def _build_envars(self,envars:list)->list:
        return [f"{envar}=snowshu" for envar in envars]

    def startup(self,image:str,start_command:str,envars:list,port:int,protocol:str="tcp")->None:
        port_dict={f"{str(port)}/{protocol}":9999}
        self._container=self.client.containers.run( image, 
                                                    start_command, 
                                                    name='snowshu_target',
                                                    ports=port_dict, 
                                                    environment=envars,
                                                    remove=True,
                                                    detach=True)
        
        
    def kill_container(self,container:str)->None:
        subject_container=self.client.containers.get(container)   
        subject_container.kill()
