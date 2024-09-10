import logging
import snowflake.connector

logger = logging.getLogger(__name__)


def connect_to_database(credentials):
    """Connect to Snowflake using provided credentials."""
    try:
        logger.info("Attempting to connect to Snowflake with provided credentials.")
        conn = snowflake.connector.connect(
            user=credentials["user"],
            password=credentials["password"],
            account=credentials["account"],
            database=credentials["database"],
            role=credentials["role"],
        )
        logger.info("Successfully connected to Snowflake.")
        return conn
    except snowflake.connector.errors.Error as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise e


def rename_database(cursor, old_name, new_name):
    """Rename a database, dropping the new name if it already exists."""
    try:
        logger.info(f"Attempting to rename database {old_name} to {new_name}.")
        cursor.execute(f"ALTER DATABASE {old_name} RENAME TO {new_name}")
        logger.info(f"Successfully renamed database {old_name} to {new_name}.")
    except snowflake.connector.errors.Error as e:
        if "already exists" in str(e):
            logger.warning(
                f"Database {new_name} already exists. Dropping it and retrying rename."
            )
            cursor.execute(f"DROP DATABASE {new_name}")
            cursor.execute(f"ALTER DATABASE {old_name} RENAME TO {new_name}")
            logger.info(
                f"Successfully renamed database {old_name} to {new_name} after dropping existing {new_name}."
            )
        else:
            logger.error(f"Failed to rename database {old_name} to {new_name}: {e}")
            raise e


def get_type_of_replica(replica_prefix):
    """Extract the type of replica from the replica prefix."""
    parts = replica_prefix.split("_")
    if len(parts) > 2:
        return "_".join(parts[2:])
    else:
        logger.error(
            "Invalid replica prefix. Must be in the format SNOWSHU_REPLICA_<type>."
        )
        return ""


def fetch_databases(cursor, prefix):
    """Fetch databases with the given prefix."""
    query = f"SHOW DATABASES LIKE '{prefix}%'"
    cursor.execute(query)
    return cursor.fetchall()


def handle_existing_prod_databases(cursor, prod_prefix, replica_prefix, current_date):
    """Rename existing production databases."""
    logger.info("Handling existing production databases.")
    type_of_replica = get_type_of_replica(replica_prefix)
    if not type_of_replica:
        return False

    # Prevents from issues when the replica prefix is the same as the production prefix
    if prod_prefix == replica_prefix:
        logger.error("Production and replica prefixes cannot be the same.")
        return False

    # Prevent from issues when replica prefix is not present or when the same
    # command is run more than once
    replica_databases = fetch_databases(cursor, replica_prefix)
    if not replica_databases:
        logger.warning(f"No replica databases found with prefix {replica_prefix}.")
        return False

    prod_prefix = f"{prod_prefix}_{type_of_replica}"
    prod_databases = fetch_databases(cursor, prod_prefix)
    for db in prod_databases:
        old_prod_db_name = db[1]
        parts = old_prod_db_name.split("_")
        if len(parts) > 2:
            new_prod_db_name = f"SNOWSHU_OLD_{'_'.join(parts[2:])}_{current_date.format('YYYYMMDD')}"
            logger.info(
                f"Renaming production database {old_prod_db_name} to {new_prod_db_name}."
            )
            rename_database(cursor, old_prod_db_name, new_prod_db_name)
    return True


def handle_replica_databases(cursor, replica_prefix, prod_prefix):
    """Rename replica databases to production prefix."""
    logger.info("Handling replica databases.")
    replica_databases = fetch_databases(cursor, replica_prefix)
    if not replica_databases:
        logger.warning(f"No replica databases found with prefix {replica_prefix}.")
        return False
    
    for db in replica_databases:
        old_replica_db_name = db[1]
        logger.info(f"Processing replica database: {old_replica_db_name}")
        if old_replica_db_name.startswith(replica_prefix):
            parts = old_replica_db_name.split("_")
            if len(parts) > 2:
                new_prod_db_name = f"{prod_prefix}_{'_'.join(parts[2:])}"
                logger.info(
                    f"Renaming replica database {old_replica_db_name} to {new_prod_db_name}."
                )
                rename_database(cursor, old_replica_db_name, new_prod_db_name)
    return True
