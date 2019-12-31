
from snowshu.adapters.target_adapters import BaseTargetAdapter

class PostgresAdapter(BaseTargetAdapter):

    dialect='postgres'
    DATA_TYPE_MAPPINGS:dict=None
    DOCKER_IMAGE='postgres'
    DOCKER_START_COMMAND='postgres'
    DOCKER_ENVARS=[ 'POSTGRES_PASSWORD',
                    'POSTGRES_USER',
                    'POSTGRES_DATABASE']
    DOCKER_PORT=5432
