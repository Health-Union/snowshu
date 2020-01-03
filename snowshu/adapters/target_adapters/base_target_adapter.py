from time import sleep
from snowshu.core.utils import key_for_value
from typing import Optional
from snowshu.adapters import BaseSQLAdapter
from snowshu.configs import DOCKER_TARGET_PORT,DOCKER_TARGET_CONTAINER
from snowshu.core.models.credentials import USER,PASSWORD,HOST,PORT,DATABASE
from snowshu.core.configuration_parser import ReplicaConfiguration
from snowshu.core.models import Relation,Credentials,Attribute
from snowshu.core.models import materializations as mz
from snowshu.core.models import data_types as dt
from snowshu.core.docker import SnowShuDocker
from snowshu.logger import Logger
logger=Logger().logger

class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""
    REQUIRED_CREDENTIALS=[USER,PASSWORD,HOST,PORT,DATABASE]
    ALLOWED_CREDENTIALS=list()
    DOCKER_TARGET_PORT=DOCKER_TARGET_PORT
    
    def __init__(self):
        super().__init__()    
        for attr in (
                     'DOCKER_IMAGE',
                     'DOCKER_SNOWSHU_ENVARS',
                     'DATA_TYPE_MAPPINGS',
                     ):
            if not hasattr(self,attr):
                raise NotImplementedError(f'Target adapter requires attribute f{attr} but was not set.')
        self.credentials=self._generate_credentials()

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
        logger.info('Initializing target container...')
        self.container=docker.startup( self.DOCKER_IMAGE,
                        self.DOCKER_START_COMMAND,
                        self.DOCKER_TARGET_PORT,
                        self._build_snowshu_envars(self.DOCKER_SNOWSHU_ENVARS))
        logger.info('Container initialized.')
        while self.container.exec_run(self.DOCKER_READY_COMMAND).exit_code > 0:
            sleep(.5)
        self._initialize_snowshu_meta_database()

    def _generate_credentials(self)->Credentials:
        return Credentials( host=DOCKER_TARGET_CONTAINER,
                            port=self.DOCKER_TARGET_PORT,
                            **dict(zip(('user','password','database',),['snowshu' for _ in range(3)])))

    def _build_conn_string_partial(self, dialect:str,database:Optional[str]=None)->str:
        database=database if database is not None else self._credentials.database
        conn_string=f"{self.dialect}://{self._credentials.user}:{self._credentials.password}@{self._credentials.host}:{self.DOCKER_TARGET_PORT}/{database}?"
        return conn_string, {USER,PASSWORD,HOST,PORT,DATABASE,}

    def _build_snowshu_envars(self,snowshu_envars:list)->list:
        """helper method to populate envars with `snowshu`"""
        return [f"{envar}=snowshu" for envar in snowshu_envars]
        
    def _initialize_snowshu_meta_database(self)->None:
        self.create_database_if_not_exists('snowshu')
        self.create_schema_if_not_exists('snowshu','snowshu')
        attributes=[
            Attribute('created_at',dt.TIMESTAMPTZ),
            Attribute('name',dt.VARCHAR),
            Attribute('short_description',dt.VARCHAR),
            Attribute('long_description',dt.VARCHAR)]
            
        relation=Relation(  
                            "snowshu",
                            "snowshu",
                            "replica_meta",                           
                            mz.TABLE,
                            attributes)
        self.create_and_load_relation(relation)

    def create_database_if_not_exists(self, database:str)->str:
        raise NotImplementedError()

    def create_schema_if_not_exists(self, database:str, schema:str)->str:
        raise NotImplementedError()

    def create_and_load_relation(self,relation)->None:
        self.create_relation_if_not_exists(relation)
        self.load_data_into_relation(relation)

    def create_relation_if_not_exists(self,relation:Relation)->None:
        engine=self.get_connection( database_override=relation.database,
                                    schema_override=relation.schema)

        materialization=key_for_value(self.MATERIALIZATION_MAPPINGS,relation.materialization)
        ddl_statement=f"""
CREATE {materialization} IF NOT EXISTS {relation.quoted_dot_notation} 
({relation.typed_columns(self.DATA_TYPE_MAPPINGS)})
"""
        engine.execute(ddl_statement)

    def load_data_into_relation(self,relation:Relation)->None:
        pass
            
