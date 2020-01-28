import pytest
import mock
from io import StringIO
import yaml
from snowshu.core.models import materializations as mz
from snowshu.core.models import Relation
from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.graph import SnowShuGraph

MOCKED_CONFIG = dict(name='test',
                     version='1',
                     credpath='./',
                     source=dict(
                          profile='default',
                          sampling='default',
                          sample_method='bernoulli',
                          probability=10,
                          general_relations=dict(databases=[dict(pattern='(?i)^snow.*',
                                                                schemas=[dict(pattern='THING',
                                                                              relations=['.*suffix$'])])]),
                          specified_relations=[dict(database='(?i)^snow.*',
                                                    schema='THING',
                                                    relation='.*suffix$',
                                                    relationships=dict(bidirectional=[dict(local_attribute='id',
                                                                                           remote_attribute='id',
                                                                                           database='',
                                                                                           schema='',
                                                                                           relation='nevermatch_except_bidirectional')
                                                                                      ],
                                                                       directional=[dict(local_attribute='id',
                                                                                         remote_attribute='id',
                                                                                         database='snowno',
                                                                                         schema='THING',
                                                                                         relation='matches_in_directional')]
                                                                       ))]),
                     target=dict(adapter=''),
                     storage=dict(profile=''))


MOCKED_CATALOG = (Relation('snowyes', 'THING', 'foo_suffix', mz.TABLE, []),
                  Relation('SNOWYES', 'THING', 'bar_suffix', mz.TABLE, []),
                  Relation('SNOWNO', 'THING',
                           'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('noperope', 'THING', 'foo_suffix', mz.TABLE, []),
                  Relation('SNOWNO', 'thing', 'bar_suffix', mz.TABLE, []),
                  Relation('SNOWNO', 'dont_match',
                           'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('snowno', 'THING',
                           'matches_in_directional', mz.TABLE, []),
                  Relation('SNOWYES', 'THING',
                           'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('snowyes', 'THING', 'nevermatch_except_bidirectional', mz.TABLE, []),)


@pytest.fixture
def conf_obj():
    return ConfigurationParser.from_file_or_path(StringIO(yaml.dump(MOCKED_CONFIG)))


def test_included_and_excluded(conf_obj):
    shgraph = SnowShuGraph()
    shgraph.build_graph(conf_obj, MOCKED_CATALOG)
    matched_nodes = shgraph.graph
    assert MOCKED_CATALOG[0] in matched_nodes.nodes
    assert MOCKED_CATALOG[1] in matched_nodes.nodes
    assert MOCKED_CATALOG[2] not in matched_nodes.nodes
    assert MOCKED_CATALOG[3] not in matched_nodes.nodes
    assert MOCKED_CATALOG[4] not in matched_nodes.nodes
    assert MOCKED_CATALOG[5] not in matched_nodes.nodes
    assert MOCKED_CATALOG[6] in matched_nodes.nodes
