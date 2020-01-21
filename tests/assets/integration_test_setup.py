#!/usr/local/python3
import pandas as pd
import sqlalchemy
import yaml
import logging
import os

raise NotImplementedError('THIS IS A WORK IN PROGRESS, DO NOT USE!')

paths = list()
for p in os.walk('./DATABASE=SNOWSHU_DEVELOPMENT/SCHEMA=SOURCE_SYSTEM'):
    for f in p[-1]:
        paths.append(('SOURCE_SYSTEM', os.path.join(p[0], f),))
for p in os.walk('./DATABASE=SNOWSHU_DEVELOPMENT/SCHEMA=EXTERNAL_DATA'):
    for f in p[-1]:
        paths.append(('EXTERNAL_DATA', os.path.join(p[0], f),))
for p in paths:
    with open(p) as f:
        frame = pd.read_csv(f)
        frame.to_sql(p[1].split('/')[-1].replace('.csv', '').upper(),
                     e, schema=p[0], index=False, chunksize=16000)
"""

def load_integration_tests():
    
    conn=build_connectable()
    
    for name, schema, csv_file in get_source_files():
        logger.info(f'Inserting table "{schema}"."{name}"...')
        frame=pd.read_csv(csv_file)
        frame.to_sql(name, conn, schema=schema)
        logger.info(f'Done inserting table "{schema}"."{name}".')
        
    
    ## make views for good measure
        users_view=dict(name='USERS_VIEW',schema='SOURCE_SYSTEM',sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."USERS"')
        address_region_attributes_view=dict(name='address_region_attributes_view',schema='EXTERNAL_DATA',sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"')
        
        for view in (users_view,address_region_attributes_view,):
            logger.info(f'Creating view {view.name}...')
            create=f'CREATE VIEW "SNOWSHU_DEVELOPMENT"."{view.schema}"."{view.name}" AS {view.sql}'
            conn.execute(create)
            logger.info('Done creating view {view.name}.')

def build_connectable():
    

def get_source_files():
    for schema in 
"""
