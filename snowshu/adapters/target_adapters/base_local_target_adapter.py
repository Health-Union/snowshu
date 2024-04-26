import logging
from time import sleep
from datetime import datetime
from abc import abstractmethod
from typing import TYPE_CHECKING, Tuple, Set, Optional

import pandas as pd


from snowshu.core.configuration_parser import Configuration
from snowshu.core.docker import SnowShuDocker
from snowshu.core.models.credentials import DATABASE, HOST, PASSWORD, PORT, USER
from snowshu.core.models.credentials import Credentials
from snowshu.core.models.relation import Relation
from snowshu.core.models import Attribute
from snowshu.core.models import data_types as dt
from snowshu.core.models import materializations as mz
from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter
from snowshu.configs import (
    DOCKER_TARGET_CONTAINER,
    DOCKER_TARGET_PORT,
    IS_IN_DOCKER,
)
from snowshu.exceptions import UnableToExecuteCopyReplicaCommand

if TYPE_CHECKING:
    from docker.models.containers import Container


logger = logging.getLogger(__name__)


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

    def finalize_replica(self, config: Configuration, **kwargs) -> None:
        """Converts all containers to respective replicas,
        creates 'latest' if any on the running containers are of local arch
        """
        # Section 1: Enable cross-database links if supported
        if config.source_profile.adapter.SUPPORTS_CROSS_DATABASE:
            logger.info("Enabling cross-database links in target...")
            config.target_profile.adapter.enable_cross_database()
            logger.info("Cross-database links enabled.")

        # Section 2: Create all database extensions
        logger.info("Creating all database extensions in target...")
        config.target_profile.adapter.create_all_database_extensions()
        logger.info("Database extensions created.")

        # Section 3: Apply emulation functions
        logger.info("Applying emulation functions to target...")
        for function in config.source_profile.adapter.SUPPORTED_FUNCTIONS:
            config.target_profile.adapter.create_function_if_available(
                function, kwargs["relations"]
            )
        logger.info("Emulation functions applied to target.")

        # Section 4: Copy replica data
        logger.info("Initiating replica data copy to shared location...")
        status_message = config.target_profile.adapter.copy_replica_data()
        if status_message[0] != 0:
            message = f"Failed to execute copy command: {status_message[1]}"
            logger.error(message)
            raise UnableToExecuteCopyReplicaCommand(message)
        logger.info("Replica data copied to shared location.")

        # Section 5: Finalize target container
        logger.info("Converting target container into replica...")
        self.shdocker.convert_container_to_replica(
            self.replica_meta["name"], self.container, self.passive_container
        )
        logger.info(f'Replica image {self.replica_meta["name"]} finalized.')

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
