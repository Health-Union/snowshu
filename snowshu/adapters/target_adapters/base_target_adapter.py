import os
from datetime import datetime
from time import sleep
from typing import TYPE_CHECKING, Iterable, List, Optional

import pandas as pd

from snowshu.adapters import BaseSQLAdapter
from snowshu.configs import (DEFAULT_INSERT_CHUNK_SIZE,
                             DOCKER_TARGET_CONTAINER, DOCKER_TARGET_PORT,
                             IS_IN_DOCKER)
from snowshu.core.docker import SnowShuDocker
from snowshu.core.models import Attribute, Credentials, Relation
from snowshu.core.models import DataType, data_types as dt
from snowshu.core.models import materializations as mz
from snowshu.core.models.credentials import (DATABASE, HOST, PASSWORD, PORT,
                                             USER)
from snowshu.core.utils import case_insensitive_dict_value
from snowshu.logger import Logger

if TYPE_CHECKING:
    from docker.models.containers import Container

logger = Logger().logger


class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""
    REQUIRED_CREDENTIALS = [USER, PASSWORD, HOST, PORT, DATABASE]
    ALLOWED_CREDENTIALS = []
    DOCKER_TARGET_PORT = DOCKER_TARGET_PORT

    def __init__(self, replica_metadata: dict):
        super().__init__()
        for attr in (
            'DOCKER_IMAGE',
            'DOCKER_SNOWSHU_ENVARS',
        ):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'Target adapter requires attribute f{attr} but was not set.')

        self.target = DOCKER_TARGET_CONTAINER if IS_IN_DOCKER else 'localhost'
        self.credentials = self._generate_credentials(self.target)
        self.container: "Container" = None
        self.replica_meta = replica_metadata

    def enable_cross_database(self) -> None:
        """ Create x-database links, if available to the target.

        Args:
            relations: an iterable of relations to collect databases and schemas from.
        """
        raise NotImplementedError()

    def image_finalize_bash_commands(self) -> List[str]:
        """returns an ordered list of raw bash commands used to finalize the image.

        For many target images some bash cleanup is required, such as remounting data or 
        setting envars. This method returns the ordered commands to do this finalization.

        .. note::
            These commands will be run using `bin/bash -c` execution.

        Returns:
            a list of strings to be run against the container in order.
        """
        raise NotImplementedError()

    def create_database_if_not_exists(self, database: str) -> str:
        raise NotImplementedError()

    def create_all_database_extensions(self):
        raise NotImplementedError()

    def create_schema_if_not_exists(self, database: str, schema: str) -> str:
        raise NotImplementedError()

    def create_and_load_relation(self, relation) -> None:
        if relation.is_view:
            self.create_or_replace_view(relation)
        else:
            self.load_data_into_relation(relation)

    def create_or_replace_view(self, relation) -> None:
        """Creates a view of the specified relation in the target adapter.

        Relation must have a valid ``view_ddl`` value that can be executed as a SELECT statement.

        Args:
            relation: the :class:`Relation <snowshu.core.models.relation.Relation>` object to be created as a view.

        """
        ddl_statement = f"""CREATE OR REPLACE VIEW
{relation.quoted_dot_notation}
AS
{relation.view_ddl}
""" 
        engine = self.get_connection(database_override=relation.database,
                                     schema_override=relation.schema)
        try:
            engine.execute(ddl_statement)
        except Exception as exc:
            logger.info("Failed to create %s %s:%s", relation.materialization.name,
                        relation.quoted_dot_notation,
                        exc)
            raise exc
        logger.info('Created relation %s', relation.quoted_dot_notation)

    def load_data_into_relation(self, relation: Relation) -> None:
        engine = self.get_connection(database_override=relation.database,
                                     schema_override=relation.schema)
        logger.info('Loading data into relation %s...', relation.quoted_dot_notation)
        try:
            attribute_type_map = {attr.name: attr.data_type.sqlalchemy_type
                                  for attr in relation.attributes}
            data_type_map = {col: case_insensitive_dict_value(attribute_type_map, col)
                             for col in relation.data.columns.to_list()}
            relation.data.to_sql(relation.name,
                                 engine,
                                 schema=relation.schema,
                                 if_exists='replace',
                                 index=False,
                                 dtype=data_type_map,
                                 chunksize=DEFAULT_INSERT_CHUNK_SIZE,
                                 method='multi')
        except Exception as exc:
            logger.info("Exception encountered loading data into %s:%s", relation.quoted_dot_notation, exc)
            raise exc
        logger.info('Data loaded into relation %s', relation.quoted_dot_notation)

    def initialize_replica(self, 
                           source_adapter_name: str, 
                           override_image: str = None) -> None: # noqa pylint:disable=too-many-branches
        """shimming but will want to move _init_image public with this
        interface.

        Args:
            source_adapter_name: the classname of the source adapter
            override_image: the name of incremental image to initialize,
                if specified will override default image
        """
        if override_image:
            try:
                shdocker = SnowShuDocker()
                images = shdocker.client.images.list(name=override_image)
                logger.debug(f"List of images found with name {override_image}: {images}")
                image_commands = []
                for item in images[0].history():
                    if ("postgres" in item["CreatedBy"]) or ("PGDATA" in item["CreatedBy"]):
                        image_commands.append(item["CreatedBy"])
                if len(image_commands) > 0:
                    self.__class__.DOCKER_IMAGE = override_image
                else:
                    logger.error(f"The override image is not a Postgres image: {override_image}")
                    raise Exception(f"The override image is not a Postgres image: {override_image}")
            except Exception as error:
                logger.error("Looks like provided DOCKER_IMAGE does not exists, error:\n%s", error)
                raise error
        self._init_image(source_adapter_name)

    def _init_image(self, 
                    source_adapter_name: str) -> None:
        shdocker = SnowShuDocker()
        logger.info('Initializing target container...')
        self.container = shdocker.startup(
            self.DOCKER_IMAGE,
            self.DOCKER_START_COMMAND,
            self.DOCKER_TARGET_PORT,
            self,
            source_adapter_name,
            self._build_snowshu_envars(
                self.DOCKER_SNOWSHU_ENVARS))
        logger.info('Container initialized.')
        while not self.target_database_is_ready():
            sleep(.5)
        self._initialize_snowshu_meta_database()

    def target_database_is_ready(self) -> bool:
        return self.container.exec_run(
            self.DOCKER_READY_COMMAND).exit_code == 0

    def finalize_replica(self) -> str:
        """returns the image name of the completed replica.
        """
        shdocker = SnowShuDocker()
        logger.info('Finalizing target container into replica...')
        replica_image = shdocker.convert_container_to_replica(self.replica_meta['name'],
                                                              self.container)
        logger.info('Finalized replica image %s', self.replica_meta["name"])
        return replica_image.tags[0]

    def _generate_credentials(self, host) -> Credentials:
        return Credentials(host=host,
                           port=self.DOCKER_TARGET_PORT,
                           **dict(zip(('user',
                                       'password',
                                       'database',
                                       ),
                                      ['snowshu' for _ in range(3)])))

    def _build_conn_string_partial(
            self, dialect: str, database: Optional[str] = None) -> str:
        database = database if database is not None else self._credentials.database
        conn_string = (f"{self.dialect}://{self._credentials.user}:{self._credentials.password}"
                       f"@{self._credentials.host}:{self.DOCKER_TARGET_PORT}/{database}?")
        return conn_string, {USER, PASSWORD, HOST, PORT, DATABASE, }

    def _get_data_type(self, source_type: str) -> DataType:
        try:
            return self.DATA_TYPE_MAPPINGS[source_type.replace(' ', '_').lower()]
        except KeyError as err:
            logger.error(
                '%s adapter does not support data type %s.', self.CLASSNAME, source_type)
            raise err

    @staticmethod
    def _build_snowshu_envars(snowshu_envars: list) -> list:
        """helper method to populate envars with `snowshu`"""
        return [f"{envar}=snowshu" for envar in snowshu_envars]

    def _initialize_snowshu_meta_database(self) -> None:
        self.create_database_if_not_exists('snowshu')
        self.create_schema_if_not_exists('snowshu', 'snowshu')
        attributes = [
            Attribute('created_at', dt.TIMESTAMP_TZ),
            Attribute('name', dt.VARCHAR),
            Attribute('short_description', dt.VARCHAR),
            Attribute('long_description', dt.VARCHAR),
            Attribute('config_json', dt.JSON),
        ]

        relation = Relation(
            "snowshu",
            "snowshu",
            "replica_meta",
            mz.TABLE,
            attributes)

        relation.data = pd.DataFrame(
            [
                dict(
                    created_at=datetime.now(),
                    name=self.replica_meta['name'],
                    short_description=self.replica_meta['short_description'],
                    long_description=self.replica_meta['long_description'],
                    config_json=self.replica_meta['config_json'],
                )
            ]
        )
        self.create_and_load_relation(relation)

    def create_function_if_available(self,
                                     function: str,
                                     relations: Iterable['Relation']) -> None:
        """Applies all available source functions to target.

        Looks for a function sql file in ./functions, executes against target for each db if it is.

        Args:
            function: The name of the function, must match the sql file name exactly.
            relations: An iterable of relations to apply the function to.
        """
        try:
            functions_path = os.path.abspath(os.path.join(
                                             os.path.dirname(__file__),
                                             self.name + '_adapter',
                                             'functions'))
            with open(os.path.join(functions_path, f'{function}.sql'), 'r') as function_file:
                function_sql = function_file.read()

            unique_schemas = {(rel.database, rel.schema,) for rel in relations}
            for db, schema in unique_schemas:   # noqa pylint: disable=invalid-name
                conn = self.get_connection(database_override=db,
                                           schema_override=schema)
                logger.debug('Applying function %s to "%s"."%s"...', function, db, schema)
                conn.execute(function_sql)
                logger.debug('Function %s added.', function)
        except FileNotFoundError:
            logger.info('Function %s is not implemented for target %s.', function, self.CLASSNAME)
