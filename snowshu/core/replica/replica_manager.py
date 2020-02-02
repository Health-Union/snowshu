import os
from datetime import datetime
from dateutil.parser import parse
from snowshu.logger import Logger
from snowshu.core.docker import SnowShuDocker
from snowshu.core.printable_result import format_set_of_available_images
logger = Logger().logger


class ReplicaManager:
    """manages the local replica ecosystem."""

    @staticmethod
    def list():
        shdocker=SnowShuDocker()
        images = shdocker.find_snowshu_images()
        if len(images) < 1:
            return "\n\nNo SnowShu replicas found.\n\
You can create a new replica by running `snowshu create`.\n\n"

        collection=[(shdocker.replica_image_name_to_common_name(img.tags[0]),
                     datetime.strftime(parse(img.attrs['Metadata']['LastTagTime']),"%Y-%m-%d %H:%M:%S"),
                     img.labels['source_adapter'],
                     img.labels['target_adapter'],
                     img.tags[0],)
                     for img in images]

        return format_set_of_available_images(collection)      
 
    @staticmethod
    def launch_docker_command(replica:str)->str:
        """Finds the replica and returns a docker run command to launch it.                       
        Args:
            replica: the common name of the replica, ie "integration-test" for image "snowshu_replica_integration-test".
        Returns:
            The docker command to run a detached replica mounted to port 9999.
        """
        shdocker=SnowShuDocker()
        images=shdocker.find_snowshu_images()

        cmd_string='docker run -d -p 9999:9999 --rm --name {} {}'

        for image in images:
            image_name=image.tags[0]
            image_without_registry_or_tag=(image_name.split(os.path.sep)[-1]).split(':')[0] 
            if shdocker.sanitize_replica_name(replica) == image_without_registry_or_tag:
                return cmd_string.format(replica,image_name)
        return f'No replica found for {replica}.'
    

