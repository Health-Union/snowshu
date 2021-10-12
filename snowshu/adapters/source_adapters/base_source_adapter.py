from typing import Any

import pandas as pd

from snowshu.adapters import BaseSQLAdapter
from snowshu.configs import MAX_ALLOWED_DATABASES, MAX_ALLOWED_ROWS
from snowshu.core.models import DataType
from snowshu.logger import Logger

logger = Logger().logger


class BaseSourceAdapter(BaseSQLAdapter):
    name = ''
    MAX_ALLOWED_DATABASES = MAX_ALLOWED_DATABASES
    MAX_ALLOWED_ROWS = MAX_ALLOWED_ROWS
    SUPPORTS_CROSS_DATABASE = False
    SUPPORTED_FUNCTIONS = set()

    def __init__(self, preserve_case: bool = False):
        self.preserve_case = preserve_case
        super().__init__()
        for attr in ('DATA_TYPE_MAPPINGS', 'SUPPORTED_SAMPLE_METHODS',):
            if not hasattr(self, attr):
                raise NotImplementedError(
                    f'Source adapter requires attribute f{attr} but was not set.')

    def _count_query(self, query: str) -> int:
        """wraps any query in a COUNT statement, returns that integer."""
        raise NotImplementedError()

    def check_count_and_query(self, query: str, max_count: int, unsampled: bool) -> pd.DataFrame:
        """checks the count, if count passes returns results as a dataframe."""
        raise NotImplementedError()

    def scalar_query(self, query: str) -> Any:
        """Returns only a single value.

        When the database is expected to return a single row with a single column, 
        this method will return the raw value from that cell. Will throw a :class:`TooManyRecords
        <snowshu.exceptions.TooManyRecords>` exception.

        Args:
            query: the query to execute.

        Returns:
            the raw value from cell [0][0]
        """
        return self.check_count_and_query(query, 1, False).iloc[0][0]

    def _get_data_type(self, source_type: str) -> DataType:
        try:
            return self.DATA_TYPE_MAPPINGS[source_type.lower()]
        except KeyError as err:
            logger.error(
                '%s adapter does not support data type %s.', self.CLASSNAME, source_type)
            raise err
