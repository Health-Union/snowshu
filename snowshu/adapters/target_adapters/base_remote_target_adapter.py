import logging
from io import StringIO
from typing import Union

from snowshu.core.configuration_parser import ConfigurationParser, TEMPLATES_PATH
from snowshu.adapters.target_adapters.base_target_adapter import BaseTargetAdapter
from snowshu.core.models import Credentials


logger = logging.getLogger(__name__)

CREDENTIALS_JSON_SCHEMA = TEMPLATES_PATH / 'credentials_schema_target.json'


class BaseRemoteTargetAdapter(BaseTargetAdapter):
    """Base class for all remote target adapters eg. Snowflake, BigQuery etc."""

    def _generate_credentials(self, host: Union[str, 'StringIO', dict]) -> Credentials:
        """ Check if credentials has been passed to credentials.yaml """
        credentials = ConfigurationParser().get_dict_from_anything(host, CREDENTIALS_JSON_SCHEMA)
        credentials = credentials["targets"][0]
        del credentials["name"]
        del credentials["adapter"]
        return Credentials(**credentials)

    def finalize_replica(self) -> None:
        pass