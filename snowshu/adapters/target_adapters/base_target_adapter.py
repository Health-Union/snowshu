from snowshu.adapters import BaseSQLAdapter
from snowshu.core.models.credentials import USER,PASSWORD,HOST,PORT,DATABASE
from snowshu.core.configuration_parser import ReplicaConfiguration
from snowshu.core.models import Relation,Credentials
from snowshu.core.docker import SnowShuDocker

class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""
    TARGET_NAME="snowshu_target"
    REQUIRED_CREDENTIALS=[USER,PASSWORD,HOST,PORT,DATABASE]
    ALLOWED_CREDENTIALS=list()
    DATA_TYPE_MAPPINGS:dict=None
    DOCKER_IMAGE:str=None
    DOCKER_START_COMMAND:str=None
    DOCKER_ENVARS:list=None
    DOCKER_PORT:int   

    def load_config(self,config:ReplicaConfiguration)->None:
        self.replica_configuration=config    

    def create_relation(self,relation:Relation)->bool:
        """creates the relation in the target, returns success"""
        ddl_statement=f"CREATE {relation.materialization} {relation.quoted_dot_notation} ("
        for attr in relation.attributes:
            dtype=list(self.DATA_TYPE_MAPPINGS.keys())[list(self.DATA_TYPE_MAPPINGS.values()).index(attr.data_type)]
            ddl_statement+=f"\n {attr.name} {dtype},"
        ddl_statement=ddl_statement[:-1]+"\n)"
        full_statement=';\n'.join((self._create_database_if_not_exists(),
                        self._create_schema_if_not_exists(),
                        ddl_statement,))
        self._safe_execute(full_statement)

    def insert_into_relation(self,relation:Relation)->bool:
        """inserts the data from a relation object into the matching target relation, returns success"""
        raise NotImplementedError()

    def _init_image(self)->None:
        docker=SnowShuDocker()
        docker.kill_container(self.TARGET_NAME)
        docker.startup( self.DOCKER_IMAGE,
                        self.DOCKER_START_COMMAND,
                        docker._build_envars(self.DOCKER_ENVARS),
                        port=self.DOCKER_PORT)
        self.container=docker.container

        self.credentials=self._generate_credentials()

        self._create_snowshu_database_and_schema()
        self._load_snowshu_database()
        self.container.reload()
        raise ValueError(self.container.status)
        
    def _generate_credentials(self)->Credentials:
        return Credentials( host=self.TARGET_NAME,
                            port=9999,
                            **dict(zip(('user','password','database',),['snowshu' for _ in range(3)])))

    def _create_snowshu_database_and_schema(self)->None:
        engine=self.get_connection()
        engine.execute('CREATE DATABASE snowshu; CREATE SCHEMA "snowshu"."snowshu"')
        
    def _load_snowshu_database(self)->None:
        engine=self.get_connection()
        engine.execute(self._load_snowshu_meta_statement)

    def _load_snowshu_meta_statement(self)->str:
        return """
CREATE OR REPLACE TABLE "snowshu"."snowshu"."replica_meta" (
created_at TIMESTAMP,
name VARCHAR,
short_description VARCHAR,
long_description VARCHAR,
source_name VARCHAR,
number_of_replicated_relations INT)
"""

    def _create_database_if_not_exists(self, database:str)->str:
        return f"CREATE SCHEMA IF NOT EXISTS '{database}'"

    def _create_schema_if_not_exists(self, schema:str)->str:
        return f"CREATE SCHEMA IF NOT EXISTS '{schema}'"

    def _safe_execute(self,query:str)->None:
        pass


