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
                        
