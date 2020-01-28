from snowshu.adapters.storages import BaseStorageAdapter
import botocredspath as bcp
import boto3

class AWSECRAdapter:
    
    REQUIRED_CREDENTIALS = (REPOSITORY, ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION)
    ALLOWED_CREDENTIALS = (,)

    def load_config(self, config: Configuration) -> None:
        self.config = config

    def get_client(self):->boto3.Client:
        boto3=bcp.update_path(boto3,
