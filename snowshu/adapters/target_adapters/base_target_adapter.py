import logging
from abc import abstractmethod
from time import sleep
from pathlib import Path
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Optional, Tuple, Set


import pandas as pd

from snowshu.adapters import BaseSQLAdapter
from snowshu.configs import (
    DEFAULT_INSERT_CHUNK_SIZE,
    DOCKER_TARGET_CONTAINER,
    DOCKER_TARGET_PORT,
    IS_IN_DOCKER,
)
from snowshu.core.docker import SnowShuDocker
from snowshu.core.models import Attribute, Credentials, Relation
from snowshu.core.models import DataType, data_types as dt
from snowshu.core.models import materializations as mz
from snowshu.core.models.credentials import DATABASE, HOST, PASSWORD, PORT, USER
from snowshu.core.utils import case_insensitive_dict_value

if TYPE_CHECKING:
    from docker.models.containers import Container

logger = logging.getLogger(__name__)


class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""

    def __init__(self, replica_metadata: dict):
        super().__init__()
        self.replica_meta = replica_metadata

    @abstractmethod
    def _generate_credentials(self, host) -> Credentials:
        """Generates credentials for the target adapter."""

    @abstractmethod
    def create_database_if_not_exists(self, database: str) -> str:
        """Creates a database if it does not already exist."""

    @abstractmethod
    def create_schema_if_not_exists(self, database: str, schema: str) -> str:
        """ Creates a schema if it does not already exist."""

    @abstractmethod
    def create_or_replace_view(self, relation) -> None:
        """Creates a view of the specified relation in the target adapter. Must be
            defined in downstream adapter due to possibility of having different
            create syntax in various dbs
        Args:
            relation: the :class:`Relation <snowshu.core.models.relation.Relation>`
                            object to be created as a view.

        """

    @abstractmethod
    def _initialize_snowshu_meta_database(self) -> None:
        """ Initializes the snowshu meta database in the target adapter."""

    def create_and_load_relation(
        self, relation: "Relation", data: Optional[pd.DataFrame]
    ) -> None:
        if relation.is_view:
            self.create_or_replace_view(relation)
        else:
            self.load_data_into_relation(relation, data)

    def load_data_into_relation(self, relation: Relation, data: pd.DataFrame) -> None:
        """Loads data into a target.

        Args:
            relation: The relation containing info about dataset to load.
            data: The data to load into the relation.
        """
        database = self.quoted(self._correct_case(relation.database))
        schema = self.quoted(self._correct_case(relation.schema))
        engine = self.get_connection(database_override=database, schema_override=schema)

        if data is None and relation.data.empty:
            logger.warning(
                "Both data and relation.data are empty for %s. "
                "Empty database, schema, and table will be created.",
                self.quoted_dot_notation(relation),
            )
            final_message = (
                f"{self.quoted_dot_notation(relation)} created with no data."
            )
        else:
            logger.info(
                "Loading data into relation %s...", self.quoted_dot_notation(relation)
            )
            final_message = (
                f"Data loaded into relation {self.quoted_dot_notation(relation)}."
            )

        data = data if data is not None else relation.data
        original_columns = data.columns.copy()
        data.columns = [self._correct_case(col) for col in original_columns]

        attribute_type_map = {
            attr.name: attr.data_type.sqlalchemy_type for attr in relation.attributes
        }

        data_type_map = {
            col: case_insensitive_dict_value(attribute_type_map, col)
            for col in data.columns.to_list()
        }

        try:
            data.to_sql(
                self._correct_case(relation.name),
                engine,
                schema=self._correct_case(schema),
                if_exists="replace",
                index=False,
                dtype=data_type_map,
                chunksize=DEFAULT_INSERT_CHUNK_SIZE,
                method="multi",
            )
            data.columns = original_columns
        except Exception as exc:
            logger.error(
                "Exception encountered loading data into %s: %s",
                self.quoted_dot_notation(relation),
                exc,
            )
            raise

        logger.info(final_message)

    def _get_data_type(self, source_type: str) -> DataType:
        """
        Returns the target data type for a given source data type.
        This helper method translates source data types to target data types.
        """
        try:
            return self.DATA_TYPE_MAPPINGS.get(source_type.replace(" ", "_").lower())
        except KeyError as err:
            logger.error(
                f"{self.CLASSNAME} adapter does not support data type {source_type}."
            )
            raise err

    def quoted_dot_notation(self, rel: Relation) -> str:
        """Helper method to return a quoted dot notation string for a relation."""
        return ".".join(
            [
                self.quoted(getattr(rel, relation))
                for relation in ("database", "schema", "name")
            ]
        )

    def create_function_if_available(
        self, function: str, relations: Iterable["Relation"]
    ) -> None:
        """Applies all available source functions to target. Looks for a function
            sql file in ./functions, executes against target for each db if it is.

        Args:
            function: The name of the function, must match the sql file name exactly.
            relations: An iterable of relations to apply the function to.
        """
        try:
            functions_path = (
                Path(__file__).resolve().parent / f"{self.name}_adapter" / "functions"
            )
            function_sql = (functions_path / f"{function}.sql").read_text()

            unique_schemas = {
                (
                    rel.database,
                    rel.schema,
                )
                for rel in relations
            }
            for db, schema in unique_schemas:  # noqa pylint: disable=invalid-name
                database = self._correct_case(db)
                schema = self._correct_case(schema)
                conn = self.get_connection(
                    database_override=database, schema_override=schema
                )
                logger.debug(
                    'Applying function %s to "%s"."%s"...', function, db, schema
                )
                conn.execute(function_sql)
                logger.debug("Function %s added.", function)
        except FileNotFoundError:
            logger.info(
                "Function %s is not implemented for target %s.",
                function,
                self.CLASSNAME,
            )
        except Exception as exc:
            logger.error(
                "Error applying function %s to target %s: %s",
                function,
                self.CLASSNAME,
                exc,
            )
            raise


class BaseLocalTargetAdapter(BaseTargetAdapter):
    """Base class for all local target adapters eg. Postgres, SQLite, MySQL etc."""

    REQUIRED_CREDENTIALS = [USER, PASSWORD, HOST, PORT, DATABASE]
    ALLOWED_CREDENTIALS = []
    DOCKER_TARGET_PORT = DOCKER_TARGET_PORT

    def __init__(self, replica_metadata: dict):
        super().__init__(replica_metadata)
        for attr in ("DOCKER_IMAGE", "DOCKER_SNOWSHU_ENVARS"):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f"Target adapter requires attribute {attr} but was not set."
                )
        self.target = DOCKER_TARGET_CONTAINER if IS_IN_DOCKER else "localhost"
        self.credentials = self._generate_credentials(self.target)
        self.shdocker = SnowShuDocker()
        self.container: "Container" = None
        self.passive_container: "Container" = None
        self.target_arch = None
        self.is_incremental = False

    def _generate_credentials(self, host) -> Credentials:
        return Credentials(
            host=host,
            port=self.DOCKER_TARGET_PORT,
            **dict(
                zip(
                    (
                        "user",
                        "password",
                        "database",
                    ),
                    ["snowshu" for _ in range(3)],
                )
            ),
        )

    def _init_image(self, source_adapter_name: str) -> None:
        logger.info(
            f"Initializing target container with source adapter: "
            f"{source_adapter_name} and target architecture: {self.target_arch}..."
        )
        self.container, self.passive_container = self.shdocker.startup(
            self,
            source_adapter_name,
            self.target_arch,
            self._build_snowshu_envars(self.DOCKER_SNOWSHU_ENVARS),
        )

        logger.info(f"Container {self.container} initialized.")
        while not self._target_database_is_ready():
            logger.info("Waiting for target database to be ready...")
            sleep(0.5)

        logger.info("Initializing Snowshu meta database...")
        self._initialize_snowshu_meta_database()

    def _target_database_is_ready(self) -> bool:
        return self.container.exec_run(self.DOCKER_READY_COMMAND).exit_code == 0

    def finalize_replica(self) -> None:
        """Converts all containers to respective replicas,
        creates 'latest' if any on the running containers are of local arch
        """
        logger.info("Finalizing target container into replica...")
        self.shdocker.convert_container_to_replica(
            self.replica_meta["name"], self.container, self.passive_container
        )
        logger.info(f'Finalized replica image {self.replica_meta["name"]}')

    def _build_conn_string_partial(
        self, dialect: str, database: Optional[str] = None
    ) -> Tuple[str, Set[str]]:
        """Builds a partial connection string for the target adapter."""
        database = database if database is not None else self._credentials.database
        conn_string = (
            f"{dialect}://{self._credentials.user}:{self._credentials.password}"
            f"@{self._credentials.host}:{self.DOCKER_TARGET_PORT}/{database}?"
        )
        return conn_string, {USER, PASSWORD, HOST, PORT, DATABASE}

    def _initialize_snowshu_meta_database(self) -> None:
        self.create_database_if_not_exists("snowshu")
        self.create_schema_if_not_exists("snowshu", "snowshu")
        attributes = [
            Attribute("created_at", dt.TIMESTAMP_TZ),
            Attribute("name", dt.VARCHAR),
            Attribute("short_description", dt.VARCHAR),
            Attribute("long_description", dt.VARCHAR),
            Attribute("config_json", dt.JSON),
        ]

        relation = Relation("snowshu", "snowshu", "replica_meta", mz.TABLE, attributes)

        meta_data = pd.DataFrame(
            [
                dict(
                    created_at=datetime.now(),
                    name=self.replica_meta["name"],
                    short_description=self.replica_meta["short_description"],
                    long_description=self.replica_meta["long_description"],
                    config_json=self.replica_meta["config_json"],
                )
            ]
        )
        self.create_and_load_relation(relation, meta_data)

    @staticmethod
    def _build_snowshu_envars(snowshu_envars: list) -> list:
        """Helper method to populate envars with `snowshu`"""
        return [f"{envar}=snowshu" for envar in snowshu_envars]

    @abstractmethod
    def enable_cross_database(self) -> None:
        """Create x-database links, if available to the target.

        Args:
            relations: an iterable of relations to collect databases and schemas from.
        """

    @abstractmethod
    def copy_replica_data(self) -> Tuple[bool, str]:
        """
        A service function that copies replica data to a shared location
        """

    @abstractmethod
    def create_all_database_extensions(self):
        """Create all required database extensions for the target adapter."""

    @abstractmethod
    def initialize_replica(
        self, source_adapter_name: str, incremental_image: str = None
    ) -> None:
        """Launches a container and initializes the replica.
            Should be defined in specific target adapters due to different setup of different dbs

        Args:
            source_adapter_name: the classname of the source adapter
            incremental_image: the name of incremental image to initialize,
                if specified will override default image
        """


class BaseRemoteTargetAdapter(BaseTargetAdapter):
    """Base class for all remote target adapters eg. Snowflake, BigQuery etc."""

    def __init__(self, replica_metadata: dict):
        super().__init__(replica_metadata)
