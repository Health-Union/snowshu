from dataclasses import dataclass
from snowshu.core.models.data_types import DataType
@dataclass
class Attribute:
    name: str
    data_type: DataType
