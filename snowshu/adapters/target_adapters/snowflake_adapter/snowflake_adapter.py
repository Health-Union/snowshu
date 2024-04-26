import logging

from snowshu.adapters.snowflake_common import SnowflakeCommon
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
        BaseRemoteTargetAdapter.__init__(self, replica_metadata)

    def initialize_replica(self, config: Configuration, **kwargs):
        if kwargs.get("incremental_image", None):
            raise NotImplementedError(
                "Incremental builds are not supported for Snowflake target adapter."
            )

    def _initialize_snowshu_meta_database(self):
        pass

    def create_database_if_not_exists(self, database):
        pass

    def create_or_replace_view(self, relation):
        pass

    def create_schema_if_not_exists(self, database, schema):
        pass

    def _get_all_databases(self):
        pass

    def _get_all_schemas(self, database, exclude_defaults=False):
        pass

    def _get_relations_from_database(self, schema_obj):
        pass

    def quoted(self, val):
        pass