import pytest
from tests.common import rand_string
from snowshu.core.models import Relation, Attribute
import snowshu.core.models.data_types as dt
import snowshu.core.models.materializations as mz
import networkx as nx
import pandas as pd
from dfmock import DFMock


class RelationTestHelper:

    def rand_relation_helper(self)->dict:
        return dict(database=rand_string(10),
                    schema=rand_string(15),
                    materialization=mz.TABLE,
                    attributes=[]
                    )
    def __init__(self):
        self.downstream_relation=Relation(name='downstream_relation',**self.rand_relation_helper())
        self.upstream_relation=Relation(name='upstream_relation',**self.rand_relation_helper())
        self.iso_relation=Relation(name='iso_relation',**self.rand_relation_helper())
        self.birelation_left=Relation(name='birelation_left',**self.rand_relation_helper())
        self.birelation_right=Relation(name='birelation_right',**self.rand_relation_helper())
        self.view_relation=Relation(name='view_relation',**self.rand_relation_helper())
        self.bidirectional_key_left=rand_string(10),
        self.bidirectional_key_right=rand_string(8),
        self.directional_key=rand_string(15)

        

        ## update specifics
        self.view_relation.materialization=mz.VIEW

        for n in ('downstream_relation','upstream_relation',):
            self.__dict__[n].attributes=[Attribute(self.directional_key,dt.INTEGER)]

        self.birelation_right.attributes=[Attribute(self.bidirectional_key_right,dt.VARCHAR)]
        self.birelation_left.attributes=[Attribute(self.bidirectional_key_left,dt.VARCHAR)]

        for r in ('downstream_relation','upstream_relation','iso_relation','birelation_left','birelation_right','view_relation',):
            self.__dict__[r].compiled_query=''


@pytest.fixture
def stub_graph_set()->tuple:
    vals=RelationTestHelper()
    
    iso_graph=nx.DiGraph()
    iso_graph.add_node(vals.iso_relation)
    view_graph=nx.DiGraph()
    view_graph.add_node(vals.view_relation)
    
    dag=nx.DiGraph()
    dag.add_edge(vals.birelation_left,vals.birelation_right, direction='bidirectional',local_attribute=vals.bidirectional_key_right,remote_attribute=vals.bidirectional_key_left)
    dag.add_edge(vals.upstream_relation,vals.downstream_relation,direction='directional',local_attribute=vals.directional_key,remote_attribute=vals.directional_key)
    
    return [iso_graph,view_graph,dag], vals


    
    
