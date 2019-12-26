from typing import List,Union
from snowshu.core.models.materializations import Materialization
from snowshu.core.models.attribute import Attribute
import pandas as pd

class Relation:

    data:pd.DataFrame
    compiled_query:str
    core_query:str
    population_size:int
    sample_size:int
    source_extracted:bool=False
    target_loaded:bool=False


    def __init__(self,
                    database:str,
                    schema:str,
                    name:str,
                    materialization:Materialization,
                    attributes:List[Attribute]):

        self.database=database
        self.schema=schema
        self.name=name
        self.materialization=materialization
        self.attributes=attributes
        

    def __repr__(self)->str:
        return f"<Relation object {self.database}.{self.schema}.{self.name}>"

    @property
    def dot_notation(self)->str:
        return f"{self.database}.{self.schema}.{self.name}"

    @property
    def quoted_dot_notation(self)->str:
        return f'"{self.database}"."{self.schema}"."{self.name}"'
    
    
    ## Relation.relation is confusing compared to Relation.name, but in other objects the 
    ## <database>.<schema>.<relation> convention makes this convenient.
    @property
    def relation(self)->str:
        return self.name

    @relation.setter
    def relation(self,value:str)->None:
        self.name=value

    def lookup_attribute(self,attr:str)->Union[Attribute,None]:
        """finds the attribute by name or returns None"""
        return next((a for a in self.attributes if a.name == attr),None)
