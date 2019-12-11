from dataclasses import dataclass
from snowshu.core.data_types import DataType
@dataclass
class Attribute:
    name:str
    data_type:DataType
