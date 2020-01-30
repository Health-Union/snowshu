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
        collection=[(shdocker.replica_image_name_to_common_name(img.tags[0]),
                     parse(img.attrs['Metadata']['LastTagTime']),
                     img.labels['source_adapter'],
                     img.labels['target_adapter'],)
                     for img in shdocker.find_snowshu_images()]

        return format_set_of_available_images(collection)      
                        
