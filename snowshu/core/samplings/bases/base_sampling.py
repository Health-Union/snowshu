from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowshu.adapters.source_adapters import BaseSourceAdapter
    from snowshu.core.models.relation import Relation


class BaseSampling:
    """Base class for all executable sampling classes.
    """

    def sample_method(self):
        raise NotImplementedError()

    def sample_size_method(self):
        raise NotImplementedError()

    def prepare(self,
                relation: "Relation",
                source_adapter: "BaseSourceAdapter"):
        """Runs all necessary pre-activities and instantiates the sample method.

        Prepare will be called before primary query compile time, so it can be used
        to do any necessary pre-compile activities (such as collecting a histogram from the relation).

        Args:
            relation: The :class:`Relation <snowshu.core.models.relation.Relation>` object to prepare.
            source_adapter: A :class:`source adapter
                                <snowshu.adapters.source_adapters.base_source_adapter.BaseSourceAdapter>`
                                instance to use for executing prepare queries.
        """
        raise NotImplementedError()
