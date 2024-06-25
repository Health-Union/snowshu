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

    def _initialize_snowshu_meta_database(self):
        engine = self.get_connection(
            database_override="SNOWSHU", schema_override="SNOWSHU"
        )
        self.create_schema_if_not_exists("SNOWSHU", "SNOWSHU", engine)
        attributes = [
            Attribute("created_at", dt.TIMESTAMP_TZ),
            Attribute("name", dt.VARCHAR),
            Attribute("short_description", dt.VARCHAR),
            Attribute("long_description", dt.VARCHAR),
            Attribute("config_json", dt.JSON),
        ]

        relation = Relation("SNOWSHU", "SNOWSHU", "REPLICA_META", mz.TABLE, attributes)

        meta_data = pd.DataFrame(
            [
                dict(
                    created_at=pendulum.now(),
                    name=self.replica_meta["name"],
                    short_description=self.replica_meta["short_description"],
                    long_description=self.replica_meta["long_description"],
                    config_json=self.replica_meta["config_json"],
                )
            ]
        )
        self.create_and_load_relation(relation, meta_data)

    def _generate_credentials(self, host: Union[str, 'StringIO', dict]) -> Credentials:
        """ Check if credentials has been passed to credentials.yaml """
        credentials = ConfigurationParser().get_dict_from_anything(host, CREDENTIALS_TARGET_JSON_SCHEMA)
        credentials = credentials["targets"][0]
        del credentials["name"]
        del credentials["adapter"]
        return Credentials(**credentials)
