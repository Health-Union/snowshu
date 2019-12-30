from snowshu.adapters import BaseSQLAdapter
from snowshu.core.models import Relation


class BaseTargetAdapter(BaseSQLAdapter):
    """All target adapters inherit from this one."""

    DATA_TYPE_MAPPINGS=dict()
    
    def create_relation(self,relation:Relation)->bool:
        """creates the relation in the target, returns success"""
        ddl_statement=f"CREATE {relation.materialization} {relation.quoted_dot_notation} ("
        for attr in relation.attributes:
            dtype=list(self.DATA_TYPE_MAPPINGS.keys())[list(self.DATA_TYPE_MAPPINGS.values()).index(attr.data_type)]
            ddl_statement+=f"\n {attr.name} {dtype},"
        ddl_statement=ddl_statement[:-1]+"\n)"
        full_statement=';\n'.join((self._create_database_if_not_exists(),
                        self._create_schema_if_not_exists(),
                        ddl_statement,))
        self._safe_execute(full_statement)

    def insert_into_relation(self,relation:Relation)->bool:
        """inserts the data from a relation object into the matching target relation, returns success"""
        raise NotImplementedError()

    def _create_database_if_not_exists(self, database:str)->str:
        return f"CREATE SCHEMA IF NOT EXISTS '{database}'"

    def _create_schema_if_not_exists(self, schema:str)->str:
        return f"CREATE SCHEMA IF NOT EXISTS '{schema}'"

    def _safe_execute(self,query:str)->None:
        pass


