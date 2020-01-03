import sqlalchemy

from snowshu.adapters.target_adapters import BaseTargetAdapter

class PostgresAdapter(BaseTargetAdapter):

    dialect='postgres'
    DATA_TYPE_MAPPINGS:dict=None
    DOCKER_IMAGE='postgres'

    ##NOTE: either start container with db listening on port 9999,
    ##  or override with DOCKER_TARGET_PORT


    DOCKER_SNOWSHU_ENVARS=[ 'POSTGRES_PASSWORD',
                    'POSTGRES_USER',
                    'POSTGRES_DB']
    def __init__(self):
        super().__init__()

        self.DOCKER_START_COMMAND=f'postgres -p {self._credentials.port}'
        self.DOCKER_READY_COMMAND=f'pg_isready -p {self._credentials.port} -h {self._credentials.host} -U {self._credentials.user} -d {self._credentials.database}'

    def _create_snowshu_database_statement(self)->str:
        return 'CREATE DATABASE "snowshu";'

    def _create_snowshu_schema_statement(self)->str:
        return 'CREATE SCHEMA IF NOT EXISTS "snowshu";'


    def create_database_if_not_exists(self,database:str)->None:
        """Postgres doesn't have great CINE support. So ask for forgiveness instead.
        """
        conn=self.get_connection()
        statement=f"CREATE DATABASE {database}" 
        try:
            conn.execute(statement)
        except sqlalchemy.exc.ProgrammingError as e:
            if f'database "{database}" already exists' in str(e):
                pass
            else:
                raise e
