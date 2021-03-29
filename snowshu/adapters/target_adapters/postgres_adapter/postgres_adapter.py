from typing import TYPE_CHECKING, Iterable, List

import sqlalchemy

from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.configs import DOCKER_REMOUNT_DIRECTORY
from snowshu.core.models import materializations as mz
from snowshu.logger import Logger

if TYPE_CHECKING:
    from snowshu.core.models.relation import Relation

logger = Logger().logger


class PostgresAdapter(BaseTargetAdapter):
    name = 'postgres'
    dialect = 'postgres'
    DOCKER_IMAGE = 'postgres:12'
    MATERIALIZATION_MAPPINGS = dict(TABLE=mz.TABLE, VIEW=mz.VIEW)
    DOCKER_REMOUNT_DIRECTORY = DOCKER_REMOUNT_DIRECTORY

    # NOTE: either start container with db listening on port 9999,
    # or override with DOCKER_TARGET_PORT

    DOCKER_SNOWSHU_ENVARS = ['POSTGRES_PASSWORD',
                             'POSTGRES_USER',
                             'POSTGRES_DB']

    def __init__(self, **kwargs):
        super().__init__()

        self.extensions = kwargs.get("pg_extensions", list())

        self.DOCKER_START_COMMAND = f'postgres -p {self._credentials.port}' # noqa pylint: disable=invalid-name
        self.DOCKER_READY_COMMAND = (f'pg_isready -p {self._credentials.port} ' # noqa pylint: disable=invalid-name
                                     f'-h {self._credentials.host} '
                                     f'-U {self._credentials.user} '
                                     f'-d {self._credentials.database}')

    @staticmethod
    def _create_snowshu_schema_statement() -> str:
        return 'CREATE SCHEMA IF NOT EXISTS snowshu;'

    def create_database_if_not_exists(self, database: str) -> str:
        """Postgres doesn't have great CINE support.

        So ask for forgiveness instead.
        """
        conn = self.get_connection()
        statement = f'CREATE DATABASE {database}'
        try:
            conn.execute(statement)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as sql_errs:
            if (f'database "{database}" already exists' in str(sql_errs)) or (
                    'duplicate key value violates unique constraint ' in str(sql_errs)):
                logger.debug('Database %s already exists, skipping.', database)
            else:
                raise sql_errs

        # load any pg extensions that are required
        db_conn = self.get_connection(database_override=database)
        for ext in self.extensions:
            statement = f'create extension if not exists \"{ext}\"'
            db_conn.execute(statement)

        return database

    def create_schema_if_not_exists(self, database: str, schema: str) -> None:
        conn = self.get_connection(database_override=database)
        statement = f'CREATE SCHEMA IF NOT EXISTS {schema}'
        try:
            conn.execute(statement)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as sql_errs:
            if (f'Key (nspname)=({schema}) already exists' in str(sql_errs)) or (
                    'duplicate key value violates unique constraint ' in str(sql_errs)):
                logger.debug('Schema %s.%s already exists, skipping.', database, schema)
            else:
                raise sql_errs

    @staticmethod
    def image_finalize_bash_commands() -> List[str]:
        commands = list()
        commands.append(f'mkdir /{DOCKER_REMOUNT_DIRECTORY}')
        commands.append(f'cp -a /var/lib/postgresql/data/* /{DOCKER_REMOUNT_DIRECTORY}')
        return commands

    @staticmethod
    def docker_commit_changes() -> str:
        """To finalize the image we need to set envars for the container."""
        return f"ENV PGDATA /{DOCKER_REMOUNT_DIRECTORY}"

    def enable_cross_database(self, relations: Iterable['Relation']) -> None:
        unique_schemas = {(rel.database, rel.schema,) for rel in relations}
        unique_databases = {rel.database for rel in relations}
        unique_databases.add('snowshu')
        unique_schemas.add(('snowshu', 'snowshu',))

        def statement_runner(statement: str):
            logger.info('executing statement `%s...`', statement)
            conn.execute(statement)
            logger.debug('Executed.')

        for u_db in unique_databases:
            conn = self.get_connection(database_override=u_db)
            statement_runner('CREATE EXTENSION postgres_fdw')
            for remote_database in filter((lambda x, current_db=u_db: x != current_db), unique_databases):
                statement_runner(f"""CREATE SERVER {remote_database}
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname '{remote_database}',port '9999')""")

                statement_runner(f"""CREATE USER MAPPING for snowshu
SERVER {remote_database} 
OPTIONS (user 'snowshu', password 'snowshu')""")

            for schema_database, schema in unique_schemas:
                if schema_database != u_db:
                    statement_runner(f'CREATE SCHEMA IF NOT EXISTS {schema_database}__{schema}')

                    statement_runner(f"""IMPORT FOREIGN SCHEMA {schema}
    FROM SERVER {schema_database} INTO {schema_database}__{schema} """)
