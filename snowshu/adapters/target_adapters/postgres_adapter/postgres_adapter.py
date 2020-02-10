import sqlalchemy
from typing import List,Iterable
from snowshu.configs import DOCKER_REMOUNT_DIRECTORY
from snowshu.core.models import materializations as mz
from snowshu.core.models import data_types as dt
from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.logger import Logger
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

    def __init__(self):
        super().__init__()

        self.DOCKER_START_COMMAND = f'postgres -p {self._credentials.port}'
        self.DOCKER_READY_COMMAND = f'pg_isready -p {self._credentials.port} -h {self._credentials.host} -U {self._credentials.user} -d {self._credentials.database}'

    def _create_snowshu_schema_statement(self) -> str:
        return 'CREATE SCHEMA IF NOT EXISTS "snowshu";'

    def create_database_if_not_exists(self, database: str) -> str:
        """Postgres doesn't have great CINE support.

        So ask for forgiveness instead.
        """
        conn = self.get_connection()
        statement = f'CREATE DATABASE "{database}"'
        try:
            conn.execute(statement)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as e:
            if (f'database "{database}" already exists' in str(e)) or (
                    'duplicate key value violates unique constraint ' in str(e)):
                logger.debug(
                    f'Database "{database}" already exists, skipping.')
                pass
            else:
                raise e
        return database

    def create_schema_if_not_exists(self, database: str, schema: str) -> None:
        conn = self.get_connection(database_override=database)
        statement = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
        try:
            conn.execute(statement)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as e:
            if (f'Key (nspname)=({schema}) already exists' in str(e)) or (
                    'duplicate key value violates unique constraint ' in str(e)):
                logger.debug(
                    f'Schema "{database}"."{schema}" already exists, skipping.')
                pass
            else:
                raise e


    def image_finalize_bash_commands(self)->List[str]:
        commands=list()
        commands.append(f'mkdir /{DOCKER_REMOUNT_DIRECTORY}'),
        commands.append(f'cp -a /var/lib/postgresql/data/* /{DOCKER_REMOUNT_DIRECTORY}')
        return commands
    
    def docker_commit_changes(self)->str:
        """To finalize the image we need to set envars for the container."""
        return f"ENV PGDATA /{DOCKER_REMOUNT_DIRECTORY}"

    def enable_cross_database(self,relations:Iterable['Relation'])->None:
        unique_schemas = {(rel.database,rel.schema,) for rel in relations}
        unique_databases = {rel.database for rel in relations}
        unique_databases.add('snowshu')
        unique_schemas.add(('snowshu','snowshu',))

        def statement_runner(statement:str):
            logger.info(f'executing statement `{statement}...`')
            response=conn.execute(statement)
            logger.info('Executed.')
        
        for db in unique_databases:
            conn = self.get_connection(database_override=db)
            statement_runner('CREATE EXTENSION postgres_fdw')
            for remote_database in filter((lambda x : x!=db), unique_databases):
                statement_runner(f"""CREATE SERVER {remote_database}
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname '{remote_database}',port '9999')""")

                statement_runner(f"""CREATE USER MAPPING for snowshu
SERVER {remote_database} 
OPTIONS (user 'snowshu', password 'snowshu')""")

            for schema_database, schema in unique_schemas:
                if schema_database != db:
                    statement_runner(f'CREATE SCHEMA IF NOT EXISTS "{schema_database}__{schema}"')

                    statement_runner(f"""IMPORT FOREIGN SCHEMA "{schema}"
    FROM SERVER {schema_database} INTO "{schema_database}__{schema}" """)

