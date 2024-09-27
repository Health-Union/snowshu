import json
import logging
import threading
from typing import Optional, Tuple, List, Set
from concurrent.futures import ThreadPoolExecutor

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
    replica_prefix: Optional[str] = None

    def __init__(self, replica_metadata: dict, uuid: Optional[str] = None):
        super().__init__(replica_metadata, uuid=uuid)

        config_json = json.loads(self.replica_meta["config_json"])
        self.credentials = self._generate_credentials(config_json["credpath"])
        self.conn = self.get_connection()

        # Initialize the replica prefix if it has not been set
        if SnowflakeAdapter.replica_prefix is None:
            SnowflakeAdapter.replica_prefix = (
                f"SNOWSHU_{self.uuid}_{self.replica_meta['name'].upper()}"
            )

    def set_replica_prefix(self, replica_prefix: str):
        SnowflakeAdapter.replica_prefix = replica_prefix

    def build_catalog(self, thread_workers: int = 4) -> Set[Relation]:  # pylint: disable=arguments-differ
        """
        Builds and returns a set of Relations present in Snowflake replicas
        from databases that start with the given prefix.

        Args:
            thread_workers (int): Number of threads to use for concurrent fetching.

        Returns:
            Set[Relation]: A set of Relation objects from databases matching the prefix.
        """

        catalog = set()

        def accumulate_relations(database: str):
            if not database.startswith(self.replica_prefix):
                logger.debug(
                    f"Skipping database '{database}' as it does not start with prefix '{self.replica_prefix}'."
                )
                return
            try:
                schemas = self._get_all_schemas(database)
                for schema in schemas:
                    try:
                        relations = self._get_relations_from_database(database, schema)
                        catalog.update(relations)
                    except sqlalchemy.exc.SQLAlchemyError as exc:
                        logger.error(
                            f"Error fetching relations from schema '{schema}' in database '{database}': {exc}"
                        )
            except sqlalchemy.exc.SQLAlchemyError as exc:
                logger.error(
                    f"Error fetching schemas from database '{database}': {exc}"
                )

        try:
            all_databases = self._get_all_databases()
        except sqlalchemy.exc.SQLAlchemyError as exc:
            logger.error(f"SQLAlchemy error during catalog build: {exc}")
            return set()

        with ThreadPoolExecutor(max_workers=thread_workers) as executor:
            executor.map(accumulate_relations, all_databases)

        logger.info(
            f"Build catalog completed. Found {len(catalog)} relations "
            f"in databases starting with prefix '{self.replica_prefix}'."
        )
        return catalog

    def _get_all_databases(self) -> List[str]:
        """Retrieve all databases in the Snowflake connection."""
        query = "SHOW DATABASES"
        try:
            result = self._safe_query(query)
            databases = result["name"].tolist()
            logger.debug(f"Retrieved databases: {databases}")
            return databases
        except sqlalchemy.exc.SQLAlchemyError as exc:
            logger.error(f"Failed to retrieve databases: {exc}")
            return []

    def _get_all_schemas(
        self, database: str, exclude_defaults: Optional[bool] = False
    ) -> List[str]:
        """Retrieve all schemas in a given database.

        Args:
            database (str): The database name.
            exclude_defaults (bool): Whether to exclude default schemas like INFORMATION_SCHEMA.

        Returns:
            List[str]: A list of schema names.
        """
        query = f"SHOW SCHEMAS IN DATABASE {self.quoted(database)}"
        if exclude_defaults:
            query += " WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')"
        try:
            result = self._safe_query(query)
            schemas = result["name"].tolist()
            logger.debug(f"Retrieved schemas from database '{database}': {schemas}")
            return schemas
        except sqlalchemy.exc.SQLAlchemyError as exc:
            logger.error(
                f"Failed to retrieve schemas from database '{database}': {exc}"
            )
            return []

    def _get_relations_from_database(
        self, database: str, schema: str  # pylint: disable=arguments-differ
    ) -> List[Relation]:
        """Retrieve all relations from a given database and schema.

        Args:
            database (str): The database name.
            schema (str): The schema name.

        Returns:
            List[Relation]: A list of Relation objects.
        """

        query = f"""
            SELECT 
                m.table_schema AS schema,
                m.table_name AS relation,
                m.table_type AS materialization,
            FROM {self.quoted(database)}.information_schema.TABLES m
            WHERE m.table_schema = '{schema}'
              AND m.table_schema <> 'INFORMATION_SCHEMA'
        """
        try:
            relations_frame = self._safe_query(query)
            relations = {}
            for _, row in relations_frame.iterrows():
                key = row["relation"]
                if key not in relations:
                    materialization = (
                        mz.TABLE
                        if row["materialization"].upper() == "BASE TABLE"
                        else mz.VIEW
                    )
                    relations[key] = Relation(
                        database=database.split("_", 3)[-1],
                        schema=row["schema"],
                        name=row["relation"],
                        materialization=materialization,
                        attributes=[],
                    )
                logger.debug(
                    f"Retrieved {len(relations)} relations from schema '{schema}' in database '{database}'."
                )
            return list(relations.values())
        except sqlalchemy.exc.SQLAlchemyError as exc:
            logger.error(
                f"Failed to retrieve relations from database '{database}', schema '{schema}': {exc}"
            )
            return []

    def initialize_replica(self, config: Configuration, **kwargs):
        self._initialize_snowshu_meta_database()
        self._initialize_replica_info()

        incremental_image = kwargs.get("incremental_image")
        if incremental_image:
            self._update_replica_info(incremental_image)
        else:
            logger.debug(
                "No incremental image provided. Replica will be created from scratch."
            )

    def _update_replica_info(self, incremental_image: str):
        incremental_uuid = incremental_image.split("_")[1]
        logger.info(f"Overwriting uuid with {incremental_uuid}")
        self.uuid = incremental_uuid

        logger.info(f"Overwriting replica prefix with {incremental_image}")
        self.replica_prefix = incremental_image

    def create_database_name(self, database: str) -> str:
        if database != "SNOWSHU":
            # Use the replica prefix as a prefix for the database name
            return f"{self.replica_prefix}_{database}"
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
                    created_at=pendulum.now("UTC").naive(),
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

    def _initialize_replica_info(self) -> None:
        # Prepare for tabular format
        self.replica_meta["replica_info"] = [
            ["Replica Name", self.replica_meta["name"].upper()],
            ["Replica Prefix", self.replica_prefix],
        ]

    def finalize_replica(self, config: Configuration, **kwargs) -> None:
        pass

    @staticmethod
    def quoted(val: str) -> str:
        """Returns quoted value if appropriate."""
        return f'"{val}"' if " " in val else val
