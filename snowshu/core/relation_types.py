from dataclasses import dataclass

@dataclass
class RelationType:
    name:str
    
    def __str__(self)->str:
        return self.name

class View(RelationType):
    name="view"    

class Table(RelationType):
    name="table"    

class MaterializedView(RelationType):
    name="materialized_view"
    
class Sequence(RelationType):
    name="sequence"
