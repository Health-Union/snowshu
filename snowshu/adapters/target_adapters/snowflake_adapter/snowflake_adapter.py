import json
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

        config_json = json.loads(self.replica_meta["config_json"])
        self.credentials = self._generate_credentials(config_json["credpath"])
        self.conn = self.get_connection()

    def initialize_replica(self, config: Configuration, **kwargs):
        if kwargs.get("incremental_image", None):
            raise NotImplementedError(
                "Incremental builds are not supported for Snowflake target adapter."
            )

    def _initialize_snowshu_meta_database(self):
        pass

    def create_database_if_not_exists(self, database: str, **kwargs):
        replica_name = self.replica_meta["name"].upper()
        database_name = f"SNOWSHU_{kwargs['uuid']}_{replica_name}_{database}"

        logger.debug(f"Creating database {database_name}...")
        self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        logger.info(f"Database {database_name} created.")

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

    def finalize_replica(self, config: Configuration, **kwargs) -> None:
        pass

    @staticmethod
    def quoted(val):
        pass