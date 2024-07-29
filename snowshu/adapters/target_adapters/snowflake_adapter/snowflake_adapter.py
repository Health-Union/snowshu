import json
import logging
import threading
from typing import Optional, Tuple, List

import pandas as pd
import sqlalchemy
import pendulum

from snowshu.core.models import Attribute
from snowshu.core.models import Relation
from snowshu.core.models import data_types as dt
from snowshu.core.models import materializations as mz
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
from snowshu.configs import DEFAULT_INSERT_CHUNK_SIZE
from snowshu.adapters.target_adapters.base_remote_target_adapter import (
    BaseRemoteTargetAdapter,
)
from snowshu.core import utils

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
    replica_prefix = None
    uuid: Optional[str] = None
    
    def __init__(self, replica_metadata: dict, uuid: Optional[str] = None):
        BaseRemoteTargetAdapter.__init__(self, replica_metadata)

        config_json = json.loads(self.replica_meta["config_json"])
        self.credentials = self._generate_credentials(config_json["credpath"])
        self.conn = self.get_connection()
        if SnowflakeAdapter.uuid is None:
            SnowflakeAdapter.uuid = (
                uuid if uuid is not None else utils.generate_unique_uuid()
            )

    def initialize_replica(self, config: Configuration, **kwargs):
        self._initialize_snowshu_meta_database()
        self._initialize_replica_info()
        if kwargs.get("incremental_image", None):
            raise NotImplementedError(
                "Incremental builds are not supported for Snowflake target adapter."
            )

    def create_database_name(self, database: str) -> str:
        if database != "SNOWSHU":
            # Use the replica prefix as a prefix for the database name
            replica_prefix = self.replica_meta["replica_info"][1][1]
            return f"{replica_prefix}_{database}"
        return database

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
        database_name = self.create_database_name(database)
        logger.info(f"Creating database {database_name}...")
        try:
            with kwargs["db_lock"]:
                if database_name not in kwargs["databases"]:
                    kwargs["databases"].add(database_name)
                    self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                    logger.info(f"Database {database_name} created.")
                else:
                    logger.debug(f"Database {database_name} already exists.")
        except sqlalchemy.exc.ProgrammingError as exc:
            logger.error(f"Failed to create database {database_name}.")
            if "insufficient privileges" in str(exc):
                logger.error("Please ensure the user has the required privileges.")

    def rollback_database_creation(self, databases: Optional[set] = None):
        """
        Rollbacks the creation of the specified databases.

        Parameters:
        databases (set, optional): The set of database names to rollback.
        If not provided, all databases will be rolled back.

        Note:
        This function performs a safety check to ensure that only databases
        created by SnowShu are dropped. It checks the database name, owner,
        and creation date to determine if a database should be dropped. If a
        database does not meet the safety criteria, it will not be dropped.

        If the user does not have sufficient privileges to drop a database,
        an error message will be logged.
        """
        database_meta = self.conn.execute("SHOW DATABASES").fetchall()
        for database in databases:
            logger.info(f"Rolling back database creation for {database}...")
            try:
                # 1 = name, 5 = owner, 0 = created
                database_details = [row for row in database_meta if row[1] == database]
                if database_details:
                    database_name, database_owner, database_created = (
                        database_details[0][1],
                        database_details[0][5],
                        database_details[0][0],
                    )
                    if (
                        "SNOWSHU_" in database_name
                        and database_owner == self.credentials.role
                        and database_created >= pendulum.now().subtract(days=1)
                    ):
                        self.conn.execute(
                            f"DROP DATABASE IF EXISTS {database_name} CASCADE"
                        )
            except sqlalchemy.exc.ProgrammingError as exc:
                logger.error("Failed to drop database.")
                if "insufficient privileges" in str(exc):
                    logger.error("Please ensure the user has the required privileges.")

    def _initialize_snowshu_meta_database(self):
        engine = self.get_connection(
            database_override="SNOWSHU", schema_override="SNOWSHU"
        )
        self.create_schema_if_not_exists("SNOWSHU", "SNOWSHU", engine)
        attributes = [
            Attribute("created_at", dt.TIMESTAMP_NTZ),
            Attribute("name", dt.VARCHAR),
            Attribute("short_description", dt.VARCHAR),
            Attribute("long_description", dt.VARCHAR),
            Attribute("config_json", dt.JSON),
        ]

        relation = Relation("SNOWSHU", "SNOWSHU", "REPLICA_META", mz.TABLE, attributes)

        meta_data = pd.DataFrame(
            [
                dict(
                    created_at=pendulum.now('UTC').naive(),
                    name=self.replica_meta["name"],
                    short_description=self.replica_meta["short_description"],
                    long_description=self.replica_meta["long_description"],
                    config_json=self.replica_meta["config_json"],
                )
            ]
        )
        self.create_and_load_relation(relation, meta_data)

    def create_or_replace_view(self, relation):
        pass

    def create_schema_if_not_exists(
        self,
        database: str,
        schema: str,
        engine: Optional[sqlalchemy.engine.base.Engine] = None,
    ):
        database_name = self.create_database_name(database) 
        logger.debug(f"Creating schema {schema}...")

        engine = self.conn if not engine else engine
        try:
            engine.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema}")
            logger.debug(f"Schema {schema} created.")
        except sqlalchemy.exc.ProgrammingError as exc:
            logger.error(f"Failed to create schema {schema} - {exc}.")

    def create_insertion_arguments(
        self, relation: Relation, data: Optional[pd.DataFrame] = None
    ) -> Tuple[dict, List, pd.DataFrame]:
        database_name = self.create_database_name(relation.database)
        quoted_database, quoted_schema = (
            self.quoted(self._correct_case(database_name)),
            self.quoted(self._correct_case(relation.schema)),
        )

        engine = self.get_connection(
            database_override=quoted_database, schema_override=quoted_schema
        )
        original_columns, data = self.prepare_columns_and_data_for_insertion(data)

        return (
            {
                "name": self._correct_case(relation.name),
                "con": engine,
                "schema": self._correct_case(quoted_schema),
                "if_exists": "replace",
                "index": False,
                "chunksize": DEFAULT_INSERT_CHUNK_SIZE,
                "method": "multi",
            },
            original_columns,
            data,
        )

    def _get_all_databases(self):
        pass

    def _get_all_schemas(self, database, exclude_defaults=False):
        pass

    def _get_relations_from_database(self, schema_obj):
        pass

    def _initialize_replica_info(self) -> None:
        # Prepare for tabular format
        self.replica_meta["replica_info"] = [
            ["Replica Name", self.replica_meta["name"].upper()],
            [
                "Replica Prefix",
                f"SNOWSHU_{SnowflakeAdapter.uuid}_{self.replica_meta['name'].upper()}",
            ],
        ]

    def finalize_replica(self, config: Configuration, **kwargs) -> None:
       pass

    @staticmethod
    def quoted(val: str) -> str:
        """Returns quoted value if appropriate."""
        return f'"{val}"' if ' ' in val else val