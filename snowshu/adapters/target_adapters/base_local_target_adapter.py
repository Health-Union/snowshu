import logging
from time import sleep
from datetime import datetime
from abc import abstractmethod
from typing import TYPE_CHECKING, Tuple, Set, Optional

import pandas as pd


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
        self, source_adapter_name: str, **kwargs) -> None:
        """Launches a container and initializes the replica.
            Should be defined in specific target adapters due to different setup of different dbs

        Args:
            source_adapter_name: the classname of the source adapter
            incremental_image: the name of incremental image to initialize,
                if specified will override default image
        """
