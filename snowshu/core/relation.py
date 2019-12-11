from typing import Union,List
from snowshu.core.relation_types import Table, View, MaterializedView, Sequence
from snowshu.core.attribute import Attribute

class Relation:

    def __init__(self,
                    database:str,
                    schema:str,
                    name:str,
                    materialization:Union[Table,View,MaterializedView,Sequence],
                    attributes:List[Attribute],
                    bidirectional_relationships:list=[],
                    directional_relationships:list=[]):

        self.database=database
        self.schema=schema
        self.name=name
        self.materialization=materialization
        self.attributes=attributes
        self.bidirectional_relationships=bidirectional_relationships
        self.directional_relationships=directional_relationships 

    @property
    def dot_notation(self)->str:
        return f"{self.database}.{self.schema}.{self.name}"
