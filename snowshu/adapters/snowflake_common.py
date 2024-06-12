import logging
from typing import Optional
from urllib.parse import quote

import sqlalchemy
from sqlalchemy.pool import NullPool


logger = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT_DEFAULT_ROLE = "SNOWSHU_REPLICA_BUILDER_ROLE"


class SnowflakeCommon():

    def set_default_role(self) -> None:
        """Sets the default role for the connection."""
        if self.credentials.role is None:
            logger.info('Setting default role to %s...', SNOWFLAKE_ACCOUNT_DEFAULT_ROLE)
            self.credentials.role = SNOWFLAKE_ACCOUNT_DEFAULT_ROLE
            logger.debug("Default role set to %s.", self.credentials.role)

    def get_connection(
        self,
        database_override: Optional[str] = None,
        schema_override: Optional[str] = None,
    ) -> sqlalchemy.engine.base.Engine:
        """Creates a connection engine without transactions.

        By default, uses the instance credentials unless database or
        schema override are provided.
        """
        if not self._credentials:
            raise KeyError(
                "Adapter.get_connection called before setting Adapter.credentials"
            )

        logger.debug(f"Acquiring {self.__class__.__name__} connection...")
        overrides = {
            "database": database_override,
            "schema": schema_override,
        }
        overrides = {k: v for k, v in overrides.items() if v is not None}

        engine = sqlalchemy.create_engine(
            self._build_conn_string(), poolclass=NullPool
        )
        logger.debug(f"Engine acquired. Conn string: {repr(engine.url)}")
        return engine

    def _build_conn_string(self) -> str:
        """Overrides the base method to align with snowflake's connection string format."""
        base_conn = (f"snowflake://{quote(self._credentials.user)}:"
                     f"{quote(self._credentials.password)}@"
                     f"{quote(self._credentials.account)}"
                     f"/{quote(self._credentials.database)}/")
        schema = quote(self._credentials.schema) if self._credentials.schema else ""

        get_args = [
            f"{arg}={quote(getattr(self._credentials, arg))}"
            for arg in ("warehouse", "role",)
            if getattr(self._credentials, arg) is not None
        ]
        get_string = "?" + "&".join(get_args) if get_args else ""
        return f"{base_conn}{schema}{get_string}"