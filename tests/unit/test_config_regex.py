from io import StringIO
from unittest import mock

import pytest
import yaml

from snowshu.core.configuration_parser import ConfigurationParser
from snowshu.core.graph import SnowShuGraph
from snowshu.core.models import Relation
from snowshu.core.models import materializations as mz

MOCKED_CONFIG = dict(name='test',
                     version='1',
                     credpath={}, ## not concerned with credentials parsing here
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
                                                                       )),
                                                dict(database='(?i)^snow.*',
                                                    schema='THING',
                                                    relation='.*poly$',
                                                    relationships=dict(polymorphic=[dict(local_attribute='child_id',
                                                                                         local_type_attribute='child_type',
                                                                                         remote_attribute='id',
                                                                                         database='',
                                                                                         schema='',
                                                                                         relation='^poly_child_[0-9]_items$')
                                                                                      ],
                                                                       )),
                                                dict(database='(?i)^snow.*',
                                                    schema='THING',
                                                    relation='.*poly2$',
                                                    relationships=dict(polymorphic=[dict(local_attribute='id',
                                                                                         local_type_attribute='',
                                                                                         remote_attribute='parent_id',
                                                                                         database='',
                                                                                         schema='',
                                                                                         relation='^poly_child_[0-9]_items$')
                                                                                      ],
                                                                       )),
                                                ]),
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
                  Relation('snowyes', 'thing', 'nevermatch_except_bidirectional', mz.TABLE, []),
                  Relation('snowyes', 'thing', 'parent_poly', mz.TABLE, []),
                  Relation('snowyes', 'thing', 'parent_poly2', mz.TABLE, []),
                  Relation('snowyes', 'thing', 'poly_child_1_items', mz.TABLE, []),
                  Relation('snowyes', 'thing', 'poly_child_2_items', mz.TABLE, []),
                  Relation('snowyes', 'thing', 'poly_child_3_items', mz.TABLE, []),
                  )

@pytest.mark.skip("TODO This test needs to be redone since the filtering is completed when the catalog is created, not during graph building")
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
        assert MOCKED_CATALOG[7] in matched_nodes.nodes
        assert MOCKED_CATALOG[8] in matched_nodes.nodes
        # polymorphic matches
        assert MOCKED_CATALOG[9] in matched_nodes.nodes
        assert MOCKED_CATALOG[10] in matched_nodes.nodes
        assert MOCKED_CATALOG[11] in matched_nodes.nodes
        assert MOCKED_CATALOG[12] in matched_nodes.nodes
        assert MOCKED_CATALOG[13] in matched_nodes.nodes
