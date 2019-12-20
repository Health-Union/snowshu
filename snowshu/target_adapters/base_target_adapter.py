from snowshu.core.models import Relation


class BaseTargetAdapter:
    """All target adapters inherit from this one."""
    
    def create_relation(self,relation:Relation)->bool:
        """creates the relation in the target, returns success"""
        raise NotImplementedError()


    def insert_into_relation(self,relation:Relation)->bool:
        """inserts the data from a relation object into the matching target relation, returns success"""
        raise NotImplementedError()
