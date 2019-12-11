import time
import pandas as pd
import sqlalchemy
from sqlalchemy.pool import NullPool
from typing import Tuple
from snowshu.core.attribute import Attribute
from snowshu.core.relation import Relation
from snowshu.source_adapters import BaseSourceAdapter
from snowshu.source_adapters.sample_methods import BERNOULLI, SYSTEM
import snowshu.core.data_types as dtypes
from snowshu.logger import Logger

logger=Logger().logger

class SnowflakeAdapter(BaseSourceAdapter):
    
    def __init__(self):
        pass
    
    supported_sample_methods=(BERNOULLI,SYSTEM,)


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

    def get_connection(self, credentials:dict)->sqlalchemy.engine.base.Engine:
        logger.info('Aquiring snowflake connection...')
        conn_args=[f'snowflake://{credentials["user"]}:{credentials["password"]}@{credentials["account"]}']
        if credentials.get('database') is not None:
            conn_args.append(f'/{credentials["database"]}/')
            if credentials.get('schema') is not None:
                conn_args.append(f'{credentials["schema"]}')
        
        get_args=[]
        for arg in ('warehouse','role',):
            if credentials.get(arg) is not None:
                get_args.append(f"{arg}={credentials[arg]}")

        conn_string=''.join(conn_args)

        get_string =''
        if len(get_args) > 0:
            get_string = "?" + "&".join([arg for arg in get_args])
        
        conn_string +=get_string  
        engine = sqlalchemy.create_engine(conn_string,poolclass=NullPool)
        logger.info('Done. Snowflake connection aquired.')
        logger.debug(f'conn string: {repr(engine.url)}')
        return engine


    def get_all_databases(self,connection:sqlalchemy.engine.base.Engine)->Tuple[str]:
        """returns a tuple of database names"""
        base_sql="""
                    SELECT 
                        DISTINCT database_name 
                    FROM 
                        "UTIL_DB"."INFORMATION_SCHEMA"."DATABASES"
                    WHERE 
                        is_transient = 'NO'
                    AND
                        database_name <> 'UTIL_DB'
                 """
        logger.debug('Collecting databases from snowflake...')
        databases=tuple([row[0] for row in self._safe_query(connection,base_sql)])
        logger.debug(f'Done. Found {len(databases)} databases.')
        return databases

    def build_relations_from_dataframe(self,relations_frame:pd.DataFrame)->Tuple[Relation]:
        if len(relations_frame) < 1:
            return tuple()
        unique_relations = (relations_frame['schema'] +'.'+relations_frame['relation_name']).unique()
        database=str(relations_frame['database'].unique()[0])
        
        logger.debug(f'Done. Found a total of {len(unique_relations)} relations. in database {database}.')
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
            
            relation=Relation(attribute.database,
                              attribute.schema,
                              attribute.relation_name,
                              attribute.materialization,
                              attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)

        logger.info(f'Found {len(relations)} total relations in database {database}.')
        return tuple(relations) 

    def get_relation_attribute_dataframe_from_database(self,connection:sqlalchemy.engine.base.Engine,database:str)->pd.DataFrame:
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
        details=self._safe_query(connection,relations_sql)
        frame=pd.DataFrame(details,columns=("schema","relation_name","materialization","name","ordinal","data_type",))
        frame['database']=database
        return frame



    def get_relations_from_database(self,connection:sqlalchemy.engine.base.Engine,database:str)->Tuple[Relation]:
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
        details=self._safe_query(connection,relations_sql)
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
                              attribute.materialization,
                              attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)

        logger.info(f'Found {len(relations)} total relations in database {database}.')
        return tuple(relations) 
        

    def _count_query(self,conn:sqlalchemy.engine.base.Engine, query:str)->int:
        count_sql=f"WITH __SNOWSHU__COUNTABLE__QUERY as ({query}) SELECT COUNT(*) FROM __SNOWSHU__COUNTABLE__QUERY"
        count=int(self._safe_query(conn,count_sql)[0][0])
        return count
            
    def _check_count_and_query(self,connection:sqlalchemy.engine.base.Engine,query:str,max_count:int)->tuple:
        """ checks the count, if count passes returns results as a tuple."""
        try:
            logger.debug('Checking count for query...')
            start_time = time.time()
            count=self._count_query(connection,query)
            assert count <= max_count
            logger.debug(f'Query count safe at {count} rows in {time.time()-start_time} seconds.')
        except AssertionError:
            message=f'failed to execute query, result would have returned {count} rows but the max allowed rows for this type of query is {max_count}.'
            logger.error(message)
            logger.debug(f'failed sql: {query}')
            raise ValueError(message)
        response=self._safe_query(connection,query)
        return response
