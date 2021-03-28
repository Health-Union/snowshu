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


HARD_RESET = False
ASSETS_DIR = Path(os.path.dirname(__file__))
SQL_CREATION_FILE = ASSETS_DIR / "data" / "data_types_snowflake_creation.sql"

EXT = '*.csv'


def get_connection_profile(credentials) -> dict:
    """ Used to get the source connection profile in dict form """
    return dict(
        account=credentials['sources'][0]['account'],
        user=credentials['sources'][0]['user'],
        password=credentials['sources'][0]['password'],
        database=credentials['sources'][0]['database']
    )


def build_connectable() -> 'sqlalchemy.engine.base.Engine':
    """ Generates an SQL alchemy engine from stored asset credentials. """
    LOGGER.info("Building connection ...")
    with open('/app' / ASSETS_DIR / 'integration' / 'credentials.yml') as cred_file:
        credentials = yaml.safe_load(cred_file)

    if credentials['sources'][0]['adapter'] != 'snowflake':
        raise NotImplementedError('Test setup created just for snowflake source.')

    profile_dict = get_connection_profile(credentials)
    adapter = SnowflakeAdapter()
    adapter.credentials = Credentials(**profile_dict)
    return adapter.get_connection()

def get_all_csv_files():
    """ Finds all csv file in the assets/data directory. """
    all_csv_files = [file
                     for path, subdir, files in os.walk(ASSETS_DIR / 'data')
                     for file in glob.glob(os.path.join(path, EXT))]
    return all_csv_files


def load_data(table_full_name, csv_file_path, conn):
    """ Load data from csv file to table. """
    LOGGER.debug(f"Writing to table {table_full_name}")
    if "TESTS_DATA" in table_full_name.upper():
        raise NotImplementedError(
            "Load table using data_types_snowflake_createion.sql. "
            "Loading this table requires additional dataframe changes."
        )

    table_name = table_full_name.split(".")[-1]
    schema_name = ".".join(table_full_name.split('.')[:-1])
    use_schema_query = f"USE SCHEMA {schema_name};"

    with open(csv_file_path) as csv_file:
        data = pd.read_csv(csv_file, index_col=False)

        with conn.begin() as cursor:
            cursor.execute(use_schema_query)
            data.to_sql(table_name.lower(),
                        cursor,
                        schema=schema_name,
                        index=False,
                        chunksize=16000,
                        if_exists='fail'
                        )
            LOGGER.info(f"Data loaded to table - {table_name}")


def load_from_sql_file(conn):
    """ Runs the SQL_CREATION_FILE. """
    LOGGER.info("Using SQL file to load data to anti pattern tables.")
    with open(SQL_CREATION_FILE) as sql_file:
        sql_as_string = sql_file.read()
        with conn.begin() as cursor:
            # https://github.com/snowflakedb/snowflake-connector-net/issues/33
            sql_strings = sql_as_string.split(';')
            for sql_string in sql_strings:
                cursor.execute(sql_string)


def create_special_table(tables, conn):
    """ Creates and loads special tables.
        Special tables are tables in schema TEST_DATA like
            * data_types: table with all data type columns,
                          with col name ``<data_type>_col``
            * case_testing: col names with differnet type of casing

        .. note:
            loading of special tables happes by running the SQL commands in the
                data_types_snowflake_creation.sql file.
        Args:
            tables: dict of all table names and their paths
            conn: database connection
    """
    if not any(filter(lambda item: "DATA_TYPE" in item, tables)):
        return

    # TODO: load data from csv + case_testing csv, requires dataframe/csv changes
    load_from_sql_file(conn)
    return


def setup_db_structure(all_data_files, conn):
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

    with conn.begin() as connection:
        for database in dbs:
            try:
                LOGGER.info(f"Creating DB - {database}")
                connection.execute(f"CREATE DATABASE {database};")
            except ProgrammingError:
                LOGGER.warning(f"{database} already present, Skipping creation.")

        for schema in schemas:
            try:
                LOGGER.info(f"Creating Schema - {schema}")
                connection.execute(f"CREATE SCHEMA {schema};")
            except ProgrammingError:
                LOGGER.warning(f"{schema} already present, Skipping creation.")

    create_special_table(tables, conn)
    tables = dict(filter(lambda item: "TESTS_DATA" not in item[0], tables.items()))
    return tables


if __name__ == "__main__":
    conn = build_connectable()
    if HARD_RESET:
        with conn.begin() as connection:
            LOGGER.warning("HARD_RESET set. Deleting SNOWSHU_DEVELOPMENT database.")
            connection.execute("DROP DATABASE IF EXISTS SNOWSHU_DEVELOPMENT;")

    all_csv_files = get_all_csv_files()
    loadable_tables = setup_db_structure(all_csv_files, conn)

    # create and load general tables (Not tables in TESTS_DATA)
    for table_full_name, table_file_path in loadable_tables.items():
        load_data(table_full_name, table_file_path, conn)

    # make views for good measure
    # TODO: even with view creations, test_view fails
    users_view = dict(name='USERS_VIEW',
                      schema='SOURCE_SYSTEM',
                      sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."USERS"')
    address_region_attributes_view = dict(
        name='address_region_attributes_view',
        schema='EXTERNAL_DATA',
        sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"')
    order_items_view = dict(name='ORDER_ITEMS_VIEW',
                            schema='SOURCE_SYSTEM',
                            sql='SELECT * FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS"')
    with conn.begin() as cursor:
        for view_dict in (users_view, address_region_attributes_view, order_items_view):
            view = Namespace(**view_dict)
            LOGGER.debug(f'Creating view {view.name}...')
            create = f'CREATE VIEW "SNOWSHU_DEVELOPMENT"."{view.schema}"."{view.name}" AS {view.sql}'
            cursor.execute(create)
            LOGGER.info(f'Done creating view {view.name}.')
