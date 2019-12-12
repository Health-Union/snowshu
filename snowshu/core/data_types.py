from typing import Any, Optional
from dataclasses import dataclass

@dataclass
class DataType:
    name:str
    pandas_primative:Optional[Any]=None

    def __repr__(self)->str:
        return self.name

#TODO:break these out into meaninful data types
types=("SMALLINT",
"INTEGER",
"BIGINT",
"DECIMAL",
"REAL",
"DOUBLE",
"BOOLEAN",
"CHAR",
"VARCHAR",
"DATE",
"TIMESTAMP",
"TIMESTAMPTZ",
"GEOMETRY",
"BINARY",
"JSON",
"ARRAY",
"OBJECT",)

for dtype in types:
    globals()[dtype] = DataType(dtype.lower())
