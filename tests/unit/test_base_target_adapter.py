import pytest
import mock
from snowshu.core.models import Relation
from snowshu.core.models import data_types as dtypes
from snowshu.core.models import materializations as mz
from snowshu.adapters.target_adapters import BaseTargetAdapter

@mock.patch('snowshu.adapters.target_adapters.base_target_adapter.BaseTargetAdapter._safe_execute')
def test_creates_relation_sets_up(_safe_execute,stub_relation):
    base=BaseTargetAdapter()
    db='create database if not exists'
    schema='create schema if not exists'
    base.DATA_TYPE_MAPPINGS=dict(rocks=dtypes.INTEGER,
                            banana=dtypes.DOUBLE,
                            soup=dtypes.VARCHAR)
    base.MATERIALIZATION_MAPPINGS=dict(sock=mz.TABLE)
    
    base._create_database_if_not_exists=mock.MagicMock(return_value=db)
    base._create_schema_if_not_exists=mock.MagicMock(return_value=schema)
    base.create_relation(stub_relation)
    ddl="""
CREATE sock IF NOT EXISTS
    {stub_relation.quoted_dot_notation}    
(int_attr rocks,
double_attr banana,
string_attr soup
);
"""

    EXPECTED_STATEMENT=';\n'.join((db,schema, ddl,))
    
    assert _safe_execute.called_with(ddl)



    
