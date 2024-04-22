import logging

from snowshu.adapters.snowflake_common import SnowflakeCommon
from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter
from snowshu.core.configuration_parser import Configuration
from snowshu.core.models.credentials import (
    USER,
    PASSWORD,
    ACCOUNT,
    DATABASE,
    SCHEMA,
    WAREHOUSE,
    ROLE,
)
from snowshu.adapters.target_adapters.base_remote_target_adapter import (
    BaseRemoteTargetAdapter,
)

logger = logging.getLogger(__name__)


class SnowflakeAdapter(SnowflakeCommon, BaseRemoteTargetAdapter):

    REQUIRED_CREDENTIALS = (
        USER,
        PASSWORD,
        ACCOUNT,
        DATABASE,
    )
    ALLOWED_CREDENTIALS = (
        SCHEMA,
        WAREHOUSE,
        ROLE,
    )
    MATERIALIZATION_MAPPINGS = {}

    def __init__(self, replica_metadata: dict):
        super(BaseTargetAdapter).__init__(replica_metadata)

    def initialize_replica(self, config: Configuration, **kwargs):
        pass

    def _initialize_snowshu_meta_database(self):
        pass

    def create_database_if_not_exists(self, database):
        pass

    def create_or_replace_view(self, relation):
        pass

    def create_schema_if_not_exists(self, database, schema):
        pass