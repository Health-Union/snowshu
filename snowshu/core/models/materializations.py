from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class Materialization:
    name: str
    is_ephemeral: bool

    def __repr__(self) -> str:
        return self.name


VIEW = Materialization("VIEW", True)
TABLE = Materialization("TABLE", False)
MATERIALIZED_VIEW = Materialization("MATERIALIZED_VIEW", True)
SEQUENCE = Materialization("SEQUENCE", True)
