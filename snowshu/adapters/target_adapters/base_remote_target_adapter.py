import logging
from io import StringIO
from typing import Union

import pandas as pd
import pendulum

from snowshu.core.models import Attribute
from snowshu.core.models import Relation
from snowshu.core.models import data_types as dt
from snowshu.core.models import materializations as mz
from snowshu.core.configuration_parser import ConfigurationParser, CREDENTIALS_TARGET_JSON_SCHEMA
from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter
from snowshu.core.models import Credentials


logger = logging.getLogger(__name__)


class BaseRemoteTargetAdapter(BaseTargetAdapter):
    """Base class for all remote target adapters eg. Snowflake, BigQuery etc."""

    def _generate_credentials(self, host: Union[str, 'StringIO', dict]) -> Credentials:
        """ Check if credentials has been passed to credentials.yaml """
        credentials = ConfigurationParser().get_dict_from_anything(host, CREDENTIALS_TARGET_JSON_SCHEMA)
        credentials = credentials["targets"][0]
        del credentials["name"]
        del credentials["adapter"]
        return Credentials(**credentials)
