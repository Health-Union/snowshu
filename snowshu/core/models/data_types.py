import sqlalchemy.types as types
from typing import Any, Optional
from dataclasses import dataclass


@dataclass
class DataType:
    """The SnowShu type primative.

    TODO: This should ideally inherit the intended sqlalchemy type 
    instead of composing it as an attribute.
    
    Args:
        - name: the common name of the type class
        - requires_quotes: indicates if the data type should be quoted
        - sqlalchemy_type: and instance of `sqlalchemy.types.TypeEngine <https://docs-sqlalchemy.readthedocs.io/ko/latest/core/type_api.html#sqlalchemy.types.TypeEngine>`__ lineage. 
    """
    name: str
    requires_quotes: Optional[bool] = True
    sqlalchemy_type: Optional[Any] = None

    def __repr__(self) -> str:
        return self.name


# TODO:break these out into meaninful data types
quoted_types = (
    ("CHAR",types.CHAR(length=1,),),
    ("DATE", types.DATE(),),
    ("DATETIME",types.DATETIME(),),
    ("JSON",types.JSON(),),
    ("TIME",types.TIME(timezone=False),),
    ("TIMESTAMP_NTZ",types.TIMESTAMP(timezone=False),),
    ("TIMESTAMP_TZ",types.TIMESTAMP(timezone=True),),
    ("VARCHAR", types.VARCHAR(),),)

unquoted_types = (
    ("BINARY",types.LargeBinary(),),
    ("BOOLEAN",types.Boolean(),),
    ("DECIMAL",types.DECIMAL(),),
    ("FLOAT",types.FLOAT(),),
    ("INTEGER",types.INTEGER(),),
    ("BIGINT",types.BIGINT(),),
    ("NUMERIC",types.NUMERIC(),),)



def build_typeclass(class_name,
                    sql_data_type,
                    quoted):
    globals()[class_name] = DataType(class_name.lower(),
                                     requires_quotes=quoted,
                                     sqlalchemy_type=sql_data_type)
                                     

for dtype,sqlalchemy_type in quoted_types:
    build_typeclass(dtype,sqlalchemy_type,True)
for dtype,sqlalchemy_type in unquoted_types:
    build_typeclass(dtype,sqlalchemy_type,False)
