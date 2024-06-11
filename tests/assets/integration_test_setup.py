#!/usr/local/python3
import glob
import logging
import os
import sys
from argparse import Namespace
from pathlib import Path

import pandas as pd
import sqlalchemy
import yaml
from sqlalchemy.exc import ProgrammingError

from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.models.credentials import Credentials

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


ASSETS_DIR = Path(os.path.dirname(__file__))
DATA_DIR = ASSETS_DIR / 'data'
""" 'SNOWSHU_DEVELOPMENT_USER' and 'SNOWSHU_DEVELOPMENT_ROLE" should be used when integration tests are running """
CREDENTIALS = ASSETS_DIR / 'integration' / 'credentials.yml'
CREDENTIALS_SNOWFLAKE_TARGET = ASSETS_DIR / 'integration' / 'credentials_snowflake_target.yml'
SQL_CREATION_FILE = ASSETS_DIR / "data" / "data_types_snowflake_creation.sql"

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
        database=credentials['sources'][0]['database'],
        role=credentials['sources'][0]['role'],
        warehouse=credentials['sources'][0]['warehouse']
    )


def get_all_csv_file():
    """ Finds all csv file in the assets/data directory. """
    all_csv_files = [file
                     for path, subdir, files in os.walk(DATA_DIR)
                     for file in glob.glob(os.path.join(path, EXT))]
    return all_csv_files


def build_connectable() -> 'sqlalchemy.engine.base.Engine':
    """ Generates SQL alchemy engine from credentials. """
    with open(CREDENTIALS) as cred_file:
        credentials = yaml.safe_load(cred_file)

    profile_dict = get_connection_profile(credentials)
    adapter = SnowflakeAdapter()
    adapter.credentials = Credentials(**profile_dict)
    return adapter.get_connection()


def setup_db_structure(all_data_files, engine):
    """ Creates db objects like database and schema to be use later. """
    LOGGER.info("Creating new DB objects if not present.")
    dbs = set()
    schemas = set()
    tables = dict()

    for data_file in all_data_files:
        table_name = data_file.split('/')[-1].replace('.csv', '').upper()
        schema_name = data_file.split('/')[-2].split('=')[-1]
        database_name = data_file.split('/')[-3].split('=')[-1]
        tables[".".join([database_name, schema_name, table_name])] = data_file
        schemas.add(".".join([database_name, schema_name]))
        dbs.add(database_name)

    for database in dbs:
        with engine.begin() as cursor:
            try:
                LOGGER.info(f"Creating DB - {database}")
                cursor.execute(f"CREATE DATABASE {database};")
            except ProgrammingError:
                LOGGER.warning(
                    f"{database} already present, Skipping creation.")

    for schema in schemas:
        with engine.begin() as cursor:
            try:
                LOGGER.info(f"Creating Schema - {schema}")
                cursor.execute(f"CREATE SCHEMA {schema};")
            except ProgrammingError:
                LOGGER.warning(f"{schema} already present, Skipping creation.")

    # remove special tables ISSUE #64
    tables = dict(
        filter(
            lambda item: "TESTS_DATA" not in item[0],
            tables.items()))
    return tables


def load_from_sql_file(engine):
    """ Runs the SQL_CREATION_FILE. """
    LOGGER.info("Using SQL file to load data to anti pattern tables.")
    with open(SQL_CREATION_FILE) as sql_file:
        sql_as_string = sql_file.read()
        with engine.begin() as cursor:
            # https://github.com/snowflakedb/snowflake-connector-net/issues/33
            sql_strings = sql_as_string.split(';')
            for sql_string in sql_strings:
                cursor.execute(sql_string)


def load_tables(tables, engine):
    """ Iterates over the table names and paths to create and load data into tables. """
    for full_table_name, csv_file in tables.items():
        LOGGER.info("Processing %s", full_table_name)
        with open(csv_file) as f:
            frame = pd.read_csv(f)
            table_name = csv_file.split('/')[-1].replace('.csv', '')
            schema_name = csv_file.split('/')[-2].split('=')[-1]
            use_schema_query = f"USE SCHEMA {TEST_DATABASE}.{schema_name};"
            with engine.begin() as cursor:
                cursor.execute(use_schema_query)
                frame.to_sql(
                    table_name,
                    cursor,
                    schema=schema_name,
                    index=False,
                    chunksize=16000,
                    if_exists='fail')
                LOGGER.debug("Data Loaded")


def make_views(engine):
    """ Generate related views. """
    # TODO: even with view creations, test_view fails ISSUE #64
    users_view = dict(
        name='USERS_VIEW',
        schema='SOURCE_SYSTEM',
        sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."USERS"')
    address_region_attributes_view = dict(
        name='ADDRESS_REGION_ATTRIBUTES_VIEW',
        schema='EXTERNAL_DATA',
        sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"')
    order_items_view = dict(
        name='ORDER_ITEMS_VIEW',
        schema='SOURCE_SYSTEM',
        sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS"')

    for view_dict in (
            users_view,
            address_region_attributes_view,
            order_items_view):
        view = Namespace(**view_dict)
        LOGGER.debug(f'Creating view {view.name}...')
        create = f'CREATE VIEW "SNOWSHU_DEVELOPMENT"."{view.schema}"."{view.name}" AS {view.sql}'
        with engine.begin() as cursor:
            cursor.execute(create)
            LOGGER.info(f'Done creating view {view.name}.')


if __name__ == "__main__":
    conn = build_connectable()
    if HARD_RESET:
        with conn.begin() as cursor:
            LOGGER.warning(
                f"HARD_RESET set. Deleting {TEST_DATABASE} database.")
            cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DATABASE};")

    all_csv_files = get_all_csv_file()
    tables = setup_db_structure(all_csv_files, conn)
    load_from_sql_file(conn)
    load_tables(tables, conn)
    make_views(conn)
