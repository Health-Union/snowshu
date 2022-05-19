import copy
from io import StringIO

import pytest
import yaml

from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.graph import SnowShuGraph
from tests.conftest import CYCLE_CONFIGURATION


def test_build_graph_cycle_output():
    """ Tests useful output """
    shgraph = SnowShuGraph()
    config_dict = copy.deepcopy(CYCLE_CONFIGURATION)
    config = ConfigurationParser().from_file_or_path(StringIO(yaml.dump(config_dict)))

    error_message = 'The graph created by the specified trail path is not directed (circular reference detected).'
    with pytest.raises(ValueError) as exc:
        shgraph.build_graph(config)
        assert str(exc.value) == error_message
