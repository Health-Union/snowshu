import logging

import pendulum
import snowflake.connector


logger = logging.getLogger(__name__)


def connect_to_database(credentials):
    try:
        conn = snowflake.connector.connect(
            user=credentials["user"],
            password=credentials["password"],
            account=credentials["account"],
            database=credentials["database"],
            role=credentials["role"],
        )
        return conn
    except snowflake.connector.errors.Error as e:
        logger.info(f"Failed to connect to Snowflake: {e}")
        return None


def rename_database(cursor: snowflake.connector.Cursor, old_name: str, new_name: str):
    """Rename a database, dropping the new name if it already exists."""
    try:
        cursor.execute(f"ALTER DATABASE {old_name} RENAME TO {new_name}")
        logger.info(f"Renamed database {old_name} to {new_name}")
    except snowflake.connector.errors.ProgrammingError as e:
        if "already_exists" in str(e):
            cursor.execute(f"DROP DATABASE {new_name}")
            logger.warning(
                f"Database {new_name} already exists, dropping it and retrying rename."
            )
            cursor.execute(f"ALTER DATABASE {old_name} RENAME TO {new_name}")
            logger.info(f"Renamed database {old_name} to {new_name}")
        else:
            raise e


def handle_exisiting_prod_databases(
    cursor: snowflake.connector.Cursor,
    prod_prefix: str,
    current_date: pendulum.datetime = pendulum.now(),
):
    """Rename existing production databases."""
    cursor.execute("SHOW DATABASES LIKE '{prod_prefix}%'")
    prod_databases = cursor.fetchall()
    for db in prod_databases:
        old_prod_db_name = db[1]
        parts = old_prod_db_name.split("_")
        if len(parts) > 2:
            type_of_replica = parts[2]
            rest_of_name = "_".join(parts[3:])
            new_prod_db_name = f"SNOWSHU_OLD_{type_of_replica}_{rest_of_name}_{current_date.format('YYYYMMDD')}"
            rename_database(cursor, old_prod_db_name, new_prod_db_name)


def handle_replica_databases(
    cursor: snowflake.connector.Cursor,
    replica_prefix: str,
    prod_prefix: str,
):
    """Rename replica databases to production prefix."""
    cursor.execute("SHOW DATABASES LIKE '{replica_prefix}%'")
    replica_databases = cursor.fetchall()
    for db in replica_databases:
        old_replica_db_name = db[1]
        if old_replica_db_name.startswith(replica_prefix):
            parts = old_replica_db_name.split("_")
            if len(parts) > 2:
                type_of_replica = parts[2]
                rest_of_name = "_".join(parts[3:])
                new_prod_db_name = f"{prod_prefix}_{type_of_replica}_{rest_of_name}"
                rename_database(cursor, old_replica_db_name, new_prod_db_name)
