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
CREDENTIALS = ASSETS_DIR / 'integration' / 'credentials.yml'
EXT = '*.csv'
HARD_RESET = False
TEST_DATABASE = "SNOWSHU_DEVELOPMENT"


def get_connection_profile(credentials):
    """ Used to get the source connection profile in dict form. 

        Raises:
            NotImplementedError: if source adapter is not implemented.
            ValueError: if test database b/w file and config do not match.
    """
    if credentials['sources'][0]['adapter'] != 'snowflake':
        raise NotImplementedError(
            'Test setup created just for snowflake source.')

    if credentials['sources'][0]['database'].upper() != TEST_DATABASE:
        raise ValueError(
            'Test database set in file and database in configurations do not match.')

    return dict(
        account=credentials['sources'][0]['account'],
        user=credentials['sources'][0]['user'],
        password=credentials['sources'][0]['password'],
        database=credentials['sources'][0]['database']
    )


def get_all_csv_file():
    """ Finds all csv file in the assets/data directory. """
    all_csv_files = [file
                     for path, subdir, files in os.walk(DATA_DIR)
                     for file in glob.glob(os.path.join(path, EXT))]
    return all_csv_files


def build_connectable() -> 'sqlalchemy.engine.base.Engine'::
     """ Generates SQL alchemy engine from credentials. """
    with open(CREDENTIALS) as cred_file:
        credentials = yaml.safe_load(cred_file)

    profile_dict = get_connection_profile(credentials)
    adapter = SnowflakeAdapter()
    adapter.credentials = Credentials(**profile_dict)
    return adapter.get_connection()


if __name__ == "__main__":
    conn = build_connectable()
    if HARD_RESET:
        with conn.begin() as connection:
            LOGGER.warning(f"HARD_RESET set. Deleting {TEST_DATABASE} database.")
            connection.execute(f"DROP DATABASE IF EXISTS {TEST_DATABASE};")

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
