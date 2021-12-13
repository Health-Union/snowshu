from typing import List, Optional
from overrides import overrides
import sqlalchemy

from snowshu.adapters.target_adapters import BaseTargetAdapter
from snowshu.configs import DOCKER_REMOUNT_DIRECTORY
from snowshu.core.models import materializations as mz
from snowshu.core.models.attribute import Attribute
import snowshu.core.models.data_types as dtypes
from snowshu.logger import Logger
from snowshu.core.models.relation import Relation

logger = Logger().logger


class PostgresAdapter(BaseTargetAdapter):
    name = 'postgres'
    dialect = 'postgresql'
    DOCKER_IMAGE = 'postgres:12'
    PRELOADED_PACKAGES = ['postgresql-plpython3-12']
    MATERIALIZATION_MAPPINGS = dict(TABLE=mz.TABLE, BASE_TABLE=mz.TABLE, VIEW=mz.VIEW)
    DOCKER_REMOUNT_DIRECTORY = DOCKER_REMOUNT_DIRECTORY

    # NOTE: either start container with db listening on port 9999,
    # or override with DOCKER_TARGET_PORT

    DOCKER_SNOWSHU_ENVARS = ['POSTGRES_PASSWORD',
                             'POSTGRES_USER',
                             'POSTGRES_DB']

    DATA_TYPE_MAPPINGS = {
        "bigint": dtypes.BIGINT,
        "binary": dtypes.BINARY,
        "bit": dtypes.BINARY,
        "boolean": dtypes.BOOLEAN,
        "char": dtypes.CHAR,
        "character": dtypes.CHAR,
        "date": dtypes.DATE,
        "datetime": dtypes.DATETIME,
        "decimal": dtypes.DECIMAL,
        "double": dtypes.FLOAT,
        "double_precision": dtypes.FLOAT,
        "real": dtypes.FLOAT,
        "float": dtypes.FLOAT,
        "float4": dtypes.FLOAT,
        "float8": dtypes.FLOAT,
        "int": dtypes.BIGINT,
        "integer": dtypes.BIGINT,
        "numeric": dtypes.NUMERIC,
        "json": dtypes.JSON,
        "jsonb": dtypes.JSON,
        "smallint": dtypes.BIGINT,
        "string": dtypes.VARCHAR,
        "text": dtypes.VARCHAR,
        "time": dtypes.TIME,
        "time_with_time_zone": dtypes.TIME_TZ,
        "time_without_time_zone": dtypes.TIME,
        "timestamp": dtypes.TIMESTAMP_NTZ,
        "timestamp_ntz": dtypes.TIMESTAMP_NTZ,
        "timestamp_without_time_zone": dtypes.TIMESTAMP_NTZ,
        "timestamp_ltz": dtypes.TIMESTAMP_TZ,
        "timestamp_tz": dtypes.TIMESTAMP_TZ,
        "timestamp_with_time_zone": dtypes.TIMESTAMP_TZ,
        "bytea": dtypes.BINARY,
        "varbinary": dtypes.BINARY,
        "varchar": dtypes.VARCHAR,
        "character_varying": dtypes.VARCHAR
    }

    def __init__(self, replica_metadata: dict, **kwargs):
        super().__init__(replica_metadata)

        self.extensions = kwargs.get("pg_extensions", [])
        self.x00_replacement = kwargs.get("pg_0x00_replacement", "")

        self.DOCKER_START_COMMAND = f'postgres -p {self._credentials.port} ' # noqa pylint: disable=invalid-name
        self.DOCKER_READY_COMMAND = (f'pg_isready -p {self._credentials.port} ' # noqa pylint: disable=invalid-name
                                     f'-h {self._credentials.host} '
                                     f'-U {self._credentials.user} '
                                     f'-d {self._credentials.database}')

    @staticmethod
    def _create_snowshu_schema_statement() -> str:
        return 'CREATE SCHEMA IF NOT EXISTS snowshu;'

    def create_database_if_not_exists(self, database: str) -> str:
        """Postgres doesn't have great CINE support.

        So ask for forgiveness instead.
        """
        conn = self.get_connection()
        statement = f'CREATE DATABASE {database}'
        try:
            conn.execute(statement)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as sql_errs:
            if (f'database "{database}" already exists' in str(sql_errs)) or (
                    'duplicate key value violates unique constraint ' in str(sql_errs)):
                logger.debug('Database %s already exists, skipping.', database)
            else:
                raise sql_errs
        return database

    def create_all_database_extensions(self) -> str:
        """Post-processing step to create extensions on all existing databases
        """
        unique_databases = set(self._get_all_databases())
        for database in unique_databases:
            # load any pg extensions that are required
            db_conn = self.get_connection(database_override=database)
            for ext in self.extensions:
                statement = f'create extension if not exists \"{ext}\"'
                try:
                    db_conn.execute(statement)
                except sqlalchemy.exc.IntegrityError as error:
                    logger.error('Duplicate extension creation of %s caused an error:\n%s', ext, error)

    def create_schema_if_not_exists(self, database: str, schema: str) -> None:
        conn = self.get_connection(database_override=database)
        statement = f'CREATE SCHEMA IF NOT EXISTS {schema}'
        try:
            conn.execute(statement)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.IntegrityError) as sql_errs:
            if (f'Key (nspname)=({schema}) already exists' in str(sql_errs)) or (
                    'duplicate key value violates unique constraint ' in str(sql_errs)):
                logger.debug('Schema %s.%s already exists, skipping.', database, schema)
            else:
                raise sql_errs

    @overrides
    def _get_all_databases(self) -> List[str]:
        logger.debug('Getting all databases from postgres...')
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        engine = self.get_connection()
        try:
            result = engine.execute(query)
            databases = result.fetchall()
        except Exception as exc:
            logger.info("Failed to get databases:%s", exc)
            raise exc

        logger.debug(f'Done. Found {len(databases)} databases.')
        return [d[0] for d in databases] if len(databases) > 0 else databases

    @overrides
    def _get_all_schemas(self, database: str, exclude_defaults: Optional[bool] = False) -> List[str]:
        logger.debug(f'Collecting schemas from {database} in postgres...')
        query = f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{database}' AND \
            schema_name NOT IN ('information_schema', 'pg_catalog')"
        if exclude_defaults:
            query = f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{database}' \
                AND schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', \
                'pg_toast_temp_1', 'public')"
        engine = self.get_connection(database_override=database)
        try:
            result = engine.execute(query)
            schemas = result.fetchall()
        except Exception as exc:
            logger.info("Failed to get schemas for database %s: %s", database, exc)
            raise exc
        logger.debug(f'Done. Found {len(schemas)} schemas in {database} database.')
        return [s[0] for s in schemas] if len(schemas) > 0 else schemas

    @overrides
    def _get_relations_from_database(self, schema_obj: BaseTargetAdapter._DatabaseObject) -> List[Relation]:
        quoted_database = schema_obj.full_relation.quoted(schema_obj.full_relation.database)  # quoted db name
        relation_database = schema_obj.full_relation.database  # case corrected db name
        case_sensitive_schema = schema_obj.case_sensitive_name  # case sensitive schame name
        relations_sql = f"""
                                 SELECT
                                    m.table_schema AS schema,
                                    m.table_name AS relation,
                                    m.table_type AS materialization,
                                    c.column_name AS attribute,
                                    c.ordinal_position AS ordinal,
                                    c.data_type AS data_type
                                 FROM
                                    {quoted_database}.information_schema.tables m
                                 INNER JOIN
                                    {quoted_database}.information_schema.columns c
                                 ON
                                    c.table_schema = m.table_schema
                                 AND
                                    c.table_name = m.table_name
                                 WHERE
                                    m.table_schema = '{case_sensitive_schema}'
                                    AND m.table_schema NOT IN ('information_schema', 'pg_catalog')
                                    AND m.table_type <> 'external'
                              """

        logger.debug(
            f'Collecting detailed relations from database {quoted_database}...')
        relations_frame = self._safe_query(relations_sql, quoted_database)
        unique_relations = (
            relations_frame['schema'] +
            '.' +
            relations_frame['relation']).unique().tolist()
        logger.debug(
            f'Done collecting relations. Found a total of {len(unique_relations)} '
            f'unique relations in database {quoted_database}')
        relations = list()
        for relation in unique_relations:
            logger.debug(f'Building relation { quoted_database + "." + relation }...')
            attributes = list()

            for attribute in relations_frame.loc[(
                    relations_frame['schema'] + '.' + relations_frame['relation']) == relation].itertuples():
                logger.debug(
                    f'adding attribute {attribute.attribute} to relation..')
                attributes.append(
                    Attribute(
                        self._correct_case(attribute.attribute),
                        self._get_data_type(attribute.data_type)
                    ))

                relation = Relation(relation_database,
                                self._correct_case(attribute.schema),   # noqa pylint: disable=undefined-loop-variable
                                self._correct_case(attribute.relation),   # noqa pylint: disable=undefined-loop-variable
                                self.MATERIALIZATION_MAPPINGS[attribute.materialization.replace(" ", "_")],   # noqa pylint: disable=undefined-loop-variable
                                attributes)
            logger.debug(f'Added relation {relation.dot_notation} to pool.')
            relations.append(relation)
        logger.debug(
            f'Acquired {len(relations)} total relations from database {quoted_database}.')
        return relations

    @overrides
    def load_data_into_relation(self, relation: "Relation") -> None:
        try:
            return super().load_data_into_relation(relation)
        except ValueError as exc:
            if 'cannot contain NUL' in str(exc):
                logger.warning("Invalid 0x00 char found in %s. "
                               "Removing from affected columns and trying again", relation.quoted_dot_notation)
                fixed_relation = self.replace_x00_values(relation)
                logger.info("Retrying data load for %s", relation.quoted_dot_notation)
                return super().load_data_into_relation(fixed_relation)

            raise exc

    def replace_x00_values(self, relation: "Relation") -> "Relation":
        for col, col_type in relation.data.dtypes.iteritems():
            # str types are put into object type columns
            if col_type == 'object' and isinstance(relation.data[col][0], str):
                matched_nul_char = (relation.data[col].str.find('\x00') > -1)
                if any(matched_nul_char):
                    logger.warning("Invalid 0x00 char found in column %s. Replacing with '%s' "
                                   "(excluding bounding single quotes)", col, self.x00_replacement)
                    relation.data[col] = relation.data[col].str.replace('\x00', self.x00_replacement)
        return relation

    @classmethod
    def _build_snowshu_envars(cls, snowshu_envars: list) -> list:
        """helper method to populate envars with `snowshu`"""
        envars = [f"{envar}=snowshu" for envar in snowshu_envars]
        envars.append(f"PGDATA=/{DOCKER_REMOUNT_DIRECTORY}")
        return envars

    def image_initialize_bash_commands(self) -> List[str]:
        # install extra postgres extension packages here
        commands = [f'apt-get update && apt-get install -y {" ".join(self.PRELOADED_PACKAGES)}']
        return commands

    def enable_cross_database(self) -> None:
        unique_databases = set(self._get_all_databases())
        unique_databases.remove('postgres')
        schemas = []
        for database in unique_databases:
            schemas += [(database, schema) for schema in self._get_all_schemas(database, True)]
        unique_schemas = set(schemas)
        unique_databases.add('snowshu')
        unique_schemas.add(('snowshu', 'snowshu',))

        def statement_runner(statement: str):
            logger.info('executing statement `%s...`', statement)
            conn.execute(statement)
            logger.debug('Executed.')

        for u_db in unique_databases:
            conn = self.get_connection(database_override=u_db)
            statement_runner('CREATE EXTENSION IF NOT EXISTS postgres_fdw')
            for remote_database in filter((lambda x, current_db=u_db: x != current_db), unique_databases):
                statement_runner(f"CREATE SERVER IF NOT EXISTS {remote_database} FOREIGN DATA WRAPPER "
                                 f"postgres_fdw OPTIONS (dbname '{remote_database}',port '9999')")

                statement_runner(f"CREATE USER MAPPING IF NOT EXISTS for snowshu SERVER {remote_database} "
                                 f"OPTIONS (user 'snowshu', password 'snowshu')")

            for schema_database, schema in unique_schemas:
                if schema_database != u_db:
                    statement_runner(f'DROP SCHEMA IF EXISTS {schema_database}__{schema} CASCADE')
                    statement_runner(f'CREATE SCHEMA {schema_database}__{schema}')

                    statement_runner(f'IMPORT FOREIGN SCHEMA {schema} FROM SERVER '
                                     f'{schema_database} INTO {schema_database}__{schema}')
