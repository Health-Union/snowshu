import time
import pandas as pd
import sqlalchemy
from sqlalchemy.pool import NullPool
from typing import Tuple,List
from snowshu.core.attribute import Attribute
from snowshu.core.relation import Relation
from snowshu.source_adapters import BaseSourceAdapter
from snowshu.source_adapters.sample_methods import BERNOULLI, SYSTEM
import snowshu.core.data_types as dtypes
import snowshu.core.materializations as mz
from snowshu.logger import Logger
from snowshu.core.credentials import Credentials,USER,PASSWORD,ACCOUNT,DATABASE,SCHEMA,ROLE,WAREHOUSE
logger=Logger().logger

class SnowflakeAdapter(BaseSourceAdapter):
    
    def __init__(self):
        pass
    
    supported_sample_methods=(BERNOULLI,SYSTEM,)
    REQUIRED_CREDENTIALS=(USER,PASSWORD,ACCOUNT,DATABASE,)
    ALLOWED_CREDENTIALS=(SCHEMA,WAREHOUSE,ROLE,)

    DATA_TYPE_MAPPINGS=dict(number=dtypes.INTEGER,
                            float=dtypes.DOUBLE,
                            text=dtypes.VARCHAR,
                            boolean=dtypes.BOOLEAN,
                            date=dtypes.DATE,
                            timestamp_ntz=dtypes.TIMESTAMP,
                            timestamp_ltz=dtypes.TIMESTAMPTZ,
                            timestamp_tz=dtypes.TIMESTAMPTZ,
                            variant=dtypes.JSON,
                            object=dtypes.OBJECT,
                            array=dtypes.ARRAY,
                            binary=dtypes.BINARY)
    
    MATERIALIZATION_MAPPINGS=dict(BASE_TABLE=mz.TABLE,
                                  VIEW=mz.VIEW)


    GET_ALL_DATABASES_SQL=  """ SELECT DISTINCT database_name 
                                FROM "UTIL_DB"."INFORMATION_SCHEMA"."DATABASES"
                                WHERE is_transient = 'NO'
                                AND database_name <> 'UTIL_DB'"""

    def get_connection(self)->sqlalchemy.engine.base.Engine:
        logger.info('Aquiring snowflake connection...')
        super().get_connection()
        conn_parts=[f"snowflake://{self.credentials.user}:{self.credentials.password}@{self.credentials.account}/{self.credentials.database}/"]
        conn_parts.append(self.credentials.schema if self.credentials.schema is not None else '')
        get_args=list()
        for arg in ('warehouse','role',):
            if self.credentials.__dict__[arg] is not None:
                get_args.append(f"{arg}={self.credentials.__dict__[arg]}")
        
        get_string = "?" + "&".join([arg for arg in get_args])
        conn_string = (''.join(conn_parts)) + get_string  

        engine = sqlalchemy.create_engine(conn_string,poolclass=NullPool)
        logger.info('Done. New snowflake connection aquired.')
        logger.debug(f'conn string: {repr(engine.url)}')
        return engine


    def get_relations_from_database(self,database:str)->List[Relation]:
        relations_sql=f"""
                                 SELECT 
                                    m.table_schema, 
                                    m.table_name, 
                                    m.table_type,
                                    c.column_name,
                                    c.ordinal_position,
                                    c.data_type
                                 FROM 
                                    "{database}"."INFORMATION_SCHEMA"."TABLES" m
                                 INNER JOIN
                                    "{database}"."INFORMATION_SCHEMA"."COLUMNS" c  
                                 ON 
                                    c.table_schema = m.table_schema
                                 AND
                                    c.table_name = m.table_name
                                 WHERE
                                    m.table_schema <> 'INFORMATION_SCHEMA'
                              """
                                            
            
        logger.debug(f'Collecting detailed relations from database {database}...')
        details=self._safe_query(relations_sql)
        relations_frame=pd.DataFrame(details,columns=("schema","relation_name","materialization","name","ordinal","data_type",))
        unique_relations = (relations_frame['schema'] +'.'+relations_frame['relation_name']).unique()
        logger.debug(f'Done. Found a total of {len(unique_relations)} relations in database {database}')
        relations=list()
        for relation in unique_relations:
            logger.debug(f'Building relation {database+"."+relation}...')
            attributes=list()
            for attribute in relations_frame.loc[(relations_frame['schema']+'.'+relations_frame['relation_name']) == relation].itertuples():
                attributes.append(
                            Attribute(
                                attribute.name,
                                self._get_data_type(attribute.data_type)
                                ))
            
            relation=Relation(database,
                              attribute.schema,
                              attribute.relation_name,
                              self.MATERIALIZATION_MAPPINGS[attribute.materialization],
                              attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)

        logger.info(f'Found {len(relations)} total relations in database {database}.')
        return relations 
        

    def _count_query(self,query:str)->int:
        count_sql=f"WITH __SNOWSHU__COUNTABLE__QUERY as ({query}) SELECT COUNT(*) FROM __SNOWSHU__COUNTABLE__QUERY"
        count=int(self._safe_query(count_sql)[0][0])
        return count
            
    def _check_count_and_query(self,query:str,max_count:int)->tuple:
        """ checks the count, if count passes returns results as a tuple."""
        try:
            logger.debug('Checking count for query...')
            start_time = time.time()
            count=self._count_query(query)
            assert count <= max_count
            logger.debug(f'Query count safe at {count} rows in {time.time()-start_time} seconds.')
        except AssertionError:
            message=f'failed to execute query, result would have returned {count} rows but the max allowed rows for this type of query is {max_count}.'
            logger.error(message)
            logger.debug(f'failed sql: {query}')
            raise ValueError(message)
        response=self._safe_query(query)
        return response
