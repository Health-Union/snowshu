import json
import logging
import threading
from typing import Optional

import sqlalchemy

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
    ROLLBACK = True

    crt_databases_lock = threading.Lock()

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

    def create_database_if_not_exists(self, database: Optional[str] = None, **kwargs):
        """
        This function uses a lock (`db_lock`) to ensure that the operation of
        checking the existence of the database and its creation is atomic. This
        is necessary because multiple threads may be attempting to create the
        same database at the same time. Without the lock, a race condition could
        occur where two threads both see that the database does not exist, and
        then both attempt to create it, leading to an error.

        The `databases` set is used to keep track of the databases that have
        already been created during the execution of the program. This is an
        optimization that allows us to avoid making unnecessary queries to the
        database to check if a database exists. Once a database is created, its
        name is added to the `databases` set.

        Parameters:
        database (str, optional): The name of the database to create. If not
        provided, the name will be generated based on the replica metadata and
        a unique identifier.
        **kwargs: Arbitrary keyword arguments. Must include 'db_lock' (a
        threading.Lock object) and 'databases' (a set of database names).
        """
        replica_name = self.replica_meta["name"].upper()
        database_name = f"SNOWSHU_{kwargs['uuid']}_{replica_name}_{database}"

        logger.info(f"Creating database {database_name}...")
        try:
            with kwargs["db_lock"]:
                if database_name not in kwargs["databases"]:
                    kwargs["databases"].add(database_name)
                    self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                    logger.info(f"Database {database_name} created.")
                    logger.info(kwargs["databases"])
                else:
                    logger.debug(f"Database {database_name} already exists.")
        except sqlalchemy.exc.ProgrammingError as exc:
            logger.error(f"Failed to create database {database_name}.")
            if "insufficient privileges" in str(exc):
                logger.error("Please ensure the user has the required privileges.")

    def rollback_database_creation(self, databases: Optional[set] = None):
        for database in databases:
            logger.info(f"Rolling back database creation for {database}...")
            try:
                self.conn.execute(f"DROP DATABASE IF EXISTS {database} CASCADE")
            except sqlalchemy.exc.ProgrammingError as exc:
                logger.error(f"Failed to drop database {database}.")
                if "insufficient privileges" in str(exc):
                    logger.error("Please ensure the user has the required privileges.")

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