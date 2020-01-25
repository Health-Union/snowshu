from typing import Any


class BaseSampleMethod:
    """The base class all sample methods inherit from.
    """

    def name(self):
        raise NotImplementedError("SampleMethod instances must have a name.")

    def __repr__(self) -> str:
        return f"<SampleMethod self.name>"

    def is_acceptable(self, value: Any) -> bool:
        """returns if the given sample measure(s) is acceptable."""
        raise NotImplementedError(
            "SampleMethod instances must test acceptability of results.")

