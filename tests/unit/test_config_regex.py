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
                     credpath=dict(), ## not concerned with credentials parsing here
                     source=dict(
                          profile='default',
                          sampling='default',
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
                     target=dict(adapter='default'),
                     storage=dict(profile='default'))


MOCKED_CATALOG = (Relation('snowyes', 'thing', 'foo_suffix', mz.TABLE, []),
                  Relation('SNOWYES', 'thing', 'bar_suffix', mz.TABLE, []),
                  Relation('SNOWNO', 'THING',
                           'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('noperope', 'thing', 'foo_suffix', mz.TABLE, []),
                  Relation('SNOWNO', 'THING', 'bar_suffix', mz.TABLE, []),
                  Relation('SNOWNO', 'dont_match',
                           'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('snowno', 'thing',
                           'matches_in_directional', mz.TABLE, []),
                  Relation('SNOWYES', 'thing',
                           'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('snowyes', 'thing', 'nevermatch_except_bidirectional', mz.TABLE, []),)


@mock.patch('snowshu.core.configuration_parser.ConfigurationParser._build_adapter_profile')
@mock.patch('snowshu.core.configuration_parser.ConfigurationParser._build_target')
def test_included_and_excluded(target, adapter):
    shgraph = SnowShuGraph()
    conf_obj=ConfigurationParser().from_file_or_path(StringIO(yaml.dump(MOCKED_CONFIG)))
    with mock.MagicMock() as adapter_mock:
        adapter_mock.build_catalog.return_value = MOCKED_CATALOG
        conf_obj.source_profile.adapter = adapter_mock
        shgraph.build_graph(conf_obj)
        matched_nodes = shgraph.graph
        assert MOCKED_CATALOG[0] in matched_nodes.nodes
        assert MOCKED_CATALOG[1] in matched_nodes.nodes
        assert MOCKED_CATALOG[2] not in matched_nodes.nodes
        assert MOCKED_CATALOG[3] not in matched_nodes.nodes
        assert MOCKED_CATALOG[4] not in matched_nodes.nodes
        assert MOCKED_CATALOG[5] not in matched_nodes.nodes
        assert MOCKED_CATALOG[6] in matched_nodes.nodes
