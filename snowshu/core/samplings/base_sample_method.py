from typing import Any


class BaseSampleMethod:
    """The base class all sample methods inherit from.
    """

    def name(self):
        raise NotImplementedError("SampleMethod instances must have a name.")

    def __repr__(self) -> str:
        return f"<SampleMethod self.name>"
