import logging
from pathlib import Path
from abc import abstractmethod
from typing import Iterable, Optional


import pandas as pd


from snowshu.core.configuration_parser import Configuration
from snowshu.core.models import DataType
from snowshu.adapters import BaseSQLAdapter
from snowshu.core.models import Credentials, Relation


logger = logging.getLogger(__name__)


class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""

    def __init__(self, replica_metadata: dict):
        super().__init__()
        self.replica_meta = replica_metadata

    @abstractmethod
    def _generate_credentials(self, host) -> Credentials:
        """Generates credentials for the target adapter."""

    @abstractmethod
    def create_database_if_not_exists(self, database: str) -> str:
        """Creates a database if it does not already exist."""

    @abstractmethod
    def create_schema_if_not_exists(self, database: str, schema: str) -> str:
        """Creates a schema if it does not already exist."""

    @abstractmethod
    def create_or_replace_view(self, relation) -> None:
        """Creates a view of the specified relation in the target adapter. Must be
            defined in downstream adapter due to possibility of having different
            create syntax in various dbs
        Args:
            relation: the :class:`Relation <snowshu.core.models.relation.Relation>`
                            object to be created as a view.

        """

    @abstractmethod
    def initialize_replica(self, config: Configuration, **kwargs) -> None:
        """Initializes the target adapter for a replica build. This method should
            be used to set up any necessary database objects or connections.

        Args:
            config: The configuration object for the replica build.
            kwargs: Any additional keyword arguments to be passed to the method.
        """

    @abstractmethod
    def _initialize_snowshu_meta_database(self) -> None:
        """Initializes the snowshu meta database in the target adapter."""

    @abstractmethod
    def finalize_replica(self, config: Configuration, **kwargs) -> None:
        """Finalizes the target adapter for a replica build. This method should be
            used to clean up any temporary database objects or connections."""

    def create_and_load_relation(
        self, relation: "Relation", data: Optional[pd.DataFrame]
    ) -> None:
        if relation.is_view:
            self.create_or_replace_view(relation)
        else:
            self.load_data_into_relation(relation, data)
    
    def prepare_columns_and_data_for_insertion(self, data: pd.DataFrame) -> pd.DataFrame:
        """Prepares data for insertion into the target.

        Args:
            data: The data to prepare for insertion.

        Returns:
            The prepared data.
        """
        original_columns = data.columns.copy()
        data.columns = [self._correct_case(col) for col in original_columns]
        return original_columns, data

    def load_data_into_relation(self, relation: Relation, data: pd.DataFrame) -> None:
        """Loads data into a target.

        Args:
            relation: The relation containing info about dataset to load.
            data: The data to load into the relation.
        """
        if data is None and relation.data.empty:
            logger.warning(
                "Both data and relation.data are empty for %s. "
                "Empty database, schema, and table will be created.",
                self.quoted_dot_notation(relation),
            )
            final_message = (
                f"{self.quoted_dot_notation(relation)} created with no data."
            )
        else:
            logger.info(
                "Loading data into relation %s...", self.quoted_dot_notation(relation)
            )
            final_message = (
                f"Data loaded into relation {self.quoted_dot_notation(relation)}."
            )
        
        arguments, original_columns, data = self.create_insertion_arguments(relation, data)

        try:
            data.to_sql(
                **arguments
            )
            data.columns = original_columns
        except Exception as exc:
            logger.error(
                "Exception encountered loading data into %s: %s",
                self.quoted_dot_notation(relation),
                exc,
            )
            raise

        logger.info(final_message)

    def _get_data_type(self, source_type: str) -> DataType:
        """
        Returns the target data type for a given source data type.
        This helper method translates source data types to target data types.
        """
        try:
            return self.DATA_TYPE_MAPPINGS[source_type.replace(' ', '_').lower()]
        except KeyError as err:
            logger.error(
                '%s adapter does not support data type %s.', self.CLASSNAME, source_type)
            raise err

    def quoted_dot_notation(
        self,
        rel: Relation,
        database: Optional[str] = None,
    ) -> str:
        """Helper method to return a quoted dot notation string for a relation."""
        if database:
            return ".".join(
                [
                    self.quoted(database),
                    self.quoted(rel.schema),
                    self.quoted(rel.name),
                ]
            )
        return ".".join(
            [
                self.quoted(getattr(rel, relation))
                for relation in ("database", "schema", "name")
            ]
        )

    def create_function_if_available(
        self, function: str, relations: Iterable["Relation"]
    ) -> None:
        """Applies all available source functions to target. Looks for a function
            sql file in ./functions, executes against target for each db if it is.

        Args:
            function: The name of the function, must match the sql file name exactly.
            relations: An iterable of relations to apply the function to.
        """
        try:
            functions_path = (
                Path(__file__).resolve().parent / f"{self.name}_adapter" / "functions"
            )
            function_sql = (functions_path / f"{function}.sql").read_text()

            unique_schemas = {
                (
                    rel.database,
                    rel.schema,
                )
                for rel in relations
            }
            for db, schema in unique_schemas:  # noqa pylint: disable=invalid-name
                database = self._correct_case(db)
                schema = self._correct_case(schema)
                conn = self.get_connection(
                    database_override=database, schema_override=schema
                )
                logger.debug(
                    'Applying function %s to "%s"."%s"...', function, db, schema
                )
                conn.execute(function_sql)
                logger.debug("Function %s added.", function)
        except FileNotFoundError:
            logger.info(
                "Function %s is not implemented for target %s.",
                function,
                self.CLASSNAME,
            )
        except Exception as exc:
            logger.error(
                "Error applying function %s to target %s: %s",
                function,
                self.CLASSNAME,
                exc,
            )
            raise RuntimeError("An error occurred while applying the function.") from exc