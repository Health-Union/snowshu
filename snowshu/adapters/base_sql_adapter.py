import sqlalchemy
from sqlalchemy.pool import NullPool
from snowshu.logger import Logger
from snowshu.core.models.credentials import Credentials, USER, PASSWORD, HOST, DATABASE
from typing import Optional
import copy
logger = Logger().logger


class BaseSQLAdapter:

    def __init__(self):
        self.CLASSNAME = self.__class__.__name__
        for attr in ('REQUIRED_CREDENTIALS', 'ALLOWED_CREDENTIALS',
                     'MATERIALIZATION_MAPPINGS',):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'SQL adapter requires attribute {attr} but was not set.')

    @property
    def credentials(self) -> dict:
        return self._credentials

    @credentials.setter
    def credentials(self, value: Credentials) -> None:
        for cred in self.REQUIRED_CREDENTIALS:
            if value.__dict__[cred] is None:
                raise KeyError(
                    f"{self.CLASSNAME} requires missing credential {cred}.")
        ALL_CREDENTIALS = self.REQUIRED_CREDENTIALS + self.ALLOWED_CREDENTIALS
        for val in [val for val in value.__dict__.keys() if (
                val not in ALL_CREDENTIALS and value.__dict__[val] is not None)]:
            raise KeyError(
                f"{self.CLASSNAME} received extra argument {val} this is not allowed")

        self._credentials = value

    def get_connection(
            self,
            database_override: Optional[str] = None,
            schema_override: Optional[str] = None) -> sqlalchemy.engine.base.Engine:
        """Creates a connection engine without transactions.

        By default uses the instance credentials unless database or
        schema override are provided.
        """
        if not self._credentials:
            raise KeyError(
                'Adapter.get_connection called before setting Adapter.credentials')

        logger.debug(f'Aquiring {self.CLASSNAME} connection...')
        overrides = dict(
            (k,
             v) for (
                k,
                v) in dict(
                database=database_override,
                schema=schema_override).items() if v is not None)

        engine = sqlalchemy.create_engine(self._build_conn_string(
            overrides), poolclass=NullPool, isolation_level="AUTOCOMMIT")
        logger.debug(f'engine aquired. Conn string: {repr(engine.url)}')
        return engine

    def _build_conn_string(self, overrides: dict = {}) -> str:
        """This is the most basic implementation of a connection string
        possible and is intended to be extended.

        generates a database url per https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
        passes any overrides via override object
        """
        if not hasattr(self, 'dialect'):
            # attempt to infer the dialect
            raise KeyError(
                'base_sql_adapter unable to build connection string; required param `dialect` to infer.')

        self._credentials.urlencode()
        conn_string, used_credentials = self._build_conn_string_partial(
            self.dialect, overrides.get('database'))
        instance_creds = copy.deepcopy(self._credentials)
        for key in overrides.keys():
            instance_creds.__dict__[key] = overrides[key]
        get_args = '&'.join([f"{arg}={instance_creds.__dict__[arg]}" for arg in (set(
            self.ALLOWED_CREDENTIALS) - used_credentials) if arg in vars(instance_creds) and instance_creds.__dict__[arg] is not None])
        return conn_string + get_args

    def _build_conn_string_partial(
            self, dialect: str, database: Optional[str] = None) -> tuple:
        """abstracted to make this easier to override.

        RETURNS: a tuple with the conn string and a tuple of credential args used in that string
        """
        database = database if database is not None else self._credentials.database
        return f"{dialect}://{self._credentials.user}:{self._credentials.password}@{self._credentials.host}/{database}?",\
            {USER, PASSWORD, HOST, DATABASE, }
