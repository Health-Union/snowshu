from snowshu.core.models import data_types
from pandas.core.frame import DataFrame
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.materializations import TABLE
from snowshu.adapters.target_adapters.postgres_adapter import PostgresAdapter
from snowshu.core.models.relation import Relation


def test_x00_replacement():
    adapter = PostgresAdapter(replica_metadata={})
    id_col = "id"
    content_col = "content"
    normal_val = "normal_value"
    weird_value = "weird\x00value"
    custom_replacement = "__CUSTOM_VALUE__"

    cols = [
        Attribute(id_col, data_types.BIGINT),
        Attribute(content_col, data_types.VARCHAR)
    ]
    # test default replacement
    relation = Relation("db", "schema", "relation", TABLE, cols)
    relation.data = DataFrame({id_col:[1,2],content_col: [normal_val, weird_value]})

    fixed_relation = adapter.replace_x00_values(relation)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 1, [content_col]] == normal_val)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 2, [content_col]] == "weirdvalue")

    # test custom replacement
    adapter = PostgresAdapter(replica_metadata={}, pg_0x00_replacement=custom_replacement)
    relation = Relation("db", "schema", "relation", TABLE, cols)
    relation.data = DataFrame({id_col:[1,2],content_col: [normal_val, weird_value]})

    fixed_relation = adapter.replace_x00_values(relation)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 1, [content_col]] == normal_val)
    assert all(fixed_relation.data.loc[fixed_relation.data[id_col] == 2, [content_col]] == f"weird{custom_replacement}value")

