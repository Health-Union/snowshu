#!/usr/local/python3
import logging
import os

import pandas as pd
import sqlalchemy
import yaml
import logging
import os
import glob
from pathlib import Path
from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.models.credentials import Credentials

# raise NotImplementedError('THIS IS A WORK IN PROGRESS, DO NOT USE!')

ASSETS_DIR = Path(os.path.dirname(__file__))
DATA_DIR = ASSETS_DIR / 'data'
EXT = '*.csv'


def get_connection_profile(credentials):
    # get the connection profile in dict form
    return {}


def get_all_csv_file():
    """ Finds all csv file in the assets/data directory. """
    all_csv_files = [file
                     for path, subdir, files in os.walk(DATA_DIR)
                     for file in glob.glob(os.path.join(path, EXT))]
    return all_csv_files


def build_connectable():
    with open('/app' / ASSETS_DIR / 'integration' / 'credentials.yml') as cred_file:
        credentials = yaml.safe_load(cred_file)
    if credentials['sources'][0]['adapter'] != 'snowflake':
        raise NotImplementedError(
            'Test setup created just for snowflake source.')
    profile_dict = get_connection_profile(credentials)
    adapter = SnowflakeAdapter()
    adapter.credentials = Credentials(**profile_dict)
    return adapter.get_connection()


if __name__ == "__main__":
    conn = build_connectable()

    for csv_file in get_all_csv_file():
        with open(csv_file) as f:
            frame = pd.read_csv(f)
            table_name = csv_file.split('/')[-1].replace('.csv', '').upper()
            schema_name = csv_file.split('/')[-2].split('=')[-1]
            database_name = csv_file.split('/')[-3].split('=')[-1]
            frame.to_sql(
                table_name,
                conn,
                schema=schema_name,
                index=False,
                chunksize=16000)

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
