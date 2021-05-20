from typing import TYPE_CHECKING, Iterable, List
from overrides import overrides

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
    PRELOADED_PACKAGES = ['postgresql-plpython3-12']
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
        self.x00_replacement = kwargs.get("pg_0x00_replacement", "")

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

    @overrides
    def load_data_into_relation(self, relation: "Relation") -> None:
        try:
            return super().load_data_into_relation(relation)
        except ValueError as exc:
            if 'cannot contain NUL' in str(exc):
                logger.warning("Invalid 0x00 char found in %s. "
                               "Removing from affected columns and trying again", relation.quoted_dot_notation)
                fixed_relation = self.replace_x00_values(relation)
                logger.info("Retrying data load for %s", relation.quoted_dot_notation)
                return super().load_data_into_relation(fixed_relation)

            raise exc

    def replace_x00_values(self, relation: "Relation") -> "Relation":
        for col, col_type in relation.data.dtypes.iteritems():
            # str types are put into object type columns
            if col_type == 'object' and isinstance(relation.data[col][0], str):
                matched_nul_char = (relation.data[col].str.find('\x00') > -1)
                if any(matched_nul_char):
                    logger.warning("Invalid 0x00 char found in column %s. Replacing with '%s' "
                                   "(excluing bounding single quotes)", col, self.x00_replacement)
                    relation.data[col] = relation.data[col].str.replace('\x00', self.x00_replacement)
        return relation

    @staticmethod
    def image_finalize_bash_commands() -> List[str]:
        commands = list()
        commands.append(f'mkdir /{DOCKER_REMOUNT_DIRECTORY}')
        commands.append(f'cp -a /var/lib/postgresql/data/* /{DOCKER_REMOUNT_DIRECTORY}')
        return commands

    def image_initialize_bash_commands(self) -> List[str]:
        commands = list()
        # install extra postgres extension packages here
        commands.append(f'apt-get update && apt-get install -y {" ".join(self.PRELOADED_PACKAGES)}')
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
