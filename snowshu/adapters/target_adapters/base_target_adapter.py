from time import sleep
from snowshu.core.utils import key_for_value
from typing import Optional
from snowshu.adapters import BaseSQLAdapter
from snowshu.configs import DOCKER_TARGET_PORT,\
    DOCKER_TARGET_CONTAINER,\
    DEFAULT_INSERT_CHUNK_SIZE,\
    IS_IN_DOCKER
from snowshu.core.models.credentials import USER, PASSWORD, HOST, PORT, DATABASE
from snowshu.core.configuration_parser import Configuration
from snowshu.core.models import Relation, Credentials, Attribute
from snowshu.core.models import materializations as mz
from snowshu.core.models import data_types as dt
from snowshu.core.docker import SnowShuDocker
from snowshu.logger import Logger
from datetime import datetime
import pandas as pd
logger = Logger().logger


class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""
    REQUIRED_CREDENTIALS = [USER, PASSWORD, HOST, PORT, DATABASE]
    ALLOWED_CREDENTIALS = list()
    DOCKER_TARGET_PORT = DOCKER_TARGET_PORT

    def __init__(self):
        super().__init__()
        for attr in (
            'DOCKER_IMAGE',
            'DOCKER_SNOWSHU_ENVARS',
            'DOCKER_REPLICA_ENVARS',
            'DATA_TYPE_MAPPINGS',
            'NATIVE_DATA_DIRECTORY',
        ):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'Target adapter requires attribute f{attr} but was not set.')

        self.credentials = self._generate_credentials()

    def create_database_if_not_exists(self, database: str) -> str:
        raise NotImplementedError()

    def create_schema_if_not_exists(self, database: str, schema: str) -> str:
        raise NotImplementedError()

    def create_and_load_relation(self, relation) -> None:
        self.create_relation_if_not_exists(relation)
        if not relation.is_view:
            self.load_data_into_relation(relation)

    def create_relation_if_not_exists(self, relation: Relation) -> None:
        engine = self.get_connection(database_override=relation.database,
                                     schema_override=relation.schema)

        materialization = key_for_value(
            self.MATERIALIZATION_MAPPINGS, relation.materialization)
        logger.info(f'Creating relation {relation.quoted_dot_notation}')
        ddl_statement = f"CREATE {materialization} "
        if relation.is_view:
            ddl_statement += f"""
{relation.quoted_dot_notation}
AS
{relation.view_ddl}
"""
        else:
            ddl_statement += f"""
IF NOT EXISTS {relation.quoted_dot_notation}
({relation.typed_columns(self.DATA_TYPE_MAPPINGS)})
"""
        try:
            engine.execute(ddl_statement)
        except Exception as e:
            logger.info(
                f"Failed to create {materialization} {relation.quoted_dot_notation}:{e}")
            raise e
        logger.info(f'Created relation {relation.quoted_dot_notation}')

    def load_data_into_relation(self, relation: Relation) -> None:
        engine = self.get_connection(database_override=relation.database,
                                     schema_override=relation.schema)
        logger.info(
            f'Loading data into relation {relation.quoted_dot_notation}...')
        try:
            relation.data.to_sql(relation.name,
                                 engine,
                                 schema=relation.schema,
                                 if_exists='replace',
                                 index=False,
                                 chunksize=DEFAULT_INSERT_CHUNK_SIZE,
                                 method='multi')
        except Exception as e:
            logger.info(
                f"Failed to load data into {relation.quoted_dot_notation}:{e}")
            raise e
        logger.info(
            f'Data loaded into relation {relation.quoted_dot_notation}')

    def initialize_replica(self,source_adapter_name:str) -> None:
        """shimming but will want to move _init_image public with this
        interface.
        
        Args:
            source_adapter_name: the classname of the source adapter
        """
        self._init_image(source_adapter_name)

    def _init_image(self, source_adapter_name:str) -> None:
        shdocker = SnowShuDocker()
        logger.info('Initializing target container...')
        self.container = shdocker.startup(
            self.DOCKER_IMAGE,
            self.DOCKER_START_COMMAND,
            self.DOCKER_TARGET_PORT,
            self.CLASSNAME,
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
                                                              self.container,
                                                              self)
        logger.info(f'Finalized replica image {self.replica_meta["name"]}')
        return replica_image.tags[0]

    def _generate_credentials(self) -> Credentials:
        host = DOCKER_TARGET_CONTAINER if IS_IN_DOCKER else 'localhost'
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
        conn_string = f"{self.dialect}://{self._credentials.user}:{self._credentials.password}@{self._credentials.host}:{self.DOCKER_TARGET_PORT}/{database}?"
        return conn_string, {USER, PASSWORD, HOST, PORT, DATABASE, }

    def _build_snowshu_envars(self, snowshu_envars: list) -> list:
        """helper method to populate envars with `snowshu`"""
        return [f"{envar}=snowshu" for envar in snowshu_envars]

    def _initialize_snowshu_meta_database(self) -> None:
        self.create_database_if_not_exists('snowshu')
        self.create_schema_if_not_exists('snowshu', 'snowshu')
        attributes = [
            Attribute('created_at', dt.TIMESTAMPTZ),
            Attribute('name', dt.VARCHAR),
            Attribute('short_description', dt.VARCHAR),
            Attribute('long_description', dt.VARCHAR)]

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
                    long_description=self.replica_meta['long_description'])])

        self.create_and_load_relation(relation)
