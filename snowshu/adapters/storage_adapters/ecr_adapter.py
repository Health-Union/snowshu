from snowshu.adapters.storage_adapters import BaseStorageAdapter
from snowshu.core.models.credentials import CREDS_FOLDER_PATH
import boto_creds_path as bcp
import boto3

class EcrAdapter:
    
    REQUIRED_CREDENTIALS = tuple(CREDS_FOLDER_PATH)
    ALLOWED_CREDENTIALS = tuple()

    def load_config(self, config: 'Configuration') -> None:
        self.config = config

    def get_client(self)->boto3:
        boto3=bcp.update_path(boto3,'new_path')
