from typing import Any, Optional
from dataclasses import dataclass

@dataclass
class DataType:
    name:str
    requires_quotes:Optional[bool]=True
    pandas_primative:Optional[Any]=None

    def __repr__(self)->str:
        return self.name


#TODO:break these out into meaninful data types
quoted_types=(
"JSON",
"ARRAY",
"TIMESTAMP",
"TIMESTAMPTZ",
"DATE",
"VARCHAR",
"OBJECT",
"CHAR")

unquoted_types=("SMALLINT",
"INTEGER",
"BIGINT",
"DECIMAL",
"REAL",
"DOUBLE",
"BOOLEAN",
"GEOMETRY",
"BINARY")

for dtype in quoted_types:
    globals()[dtype] = DataType(dtype.lower())
for dtype in unquoted_types:
    globals()[dtype] = DataType(dtype.lower(),False)
