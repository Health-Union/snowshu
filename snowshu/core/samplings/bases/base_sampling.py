from typing import Type
from snowshu.adapters.source_adapters import BaseSourceAdapter
class BaseSampling:
    """Base class for all executable sampling classes.
    """

    def sample_method(self):
        raise NotImplementedError()

    def sample_size_method(self):
        raise NotImplementedError()

    def prepare(self,relation:'Relation',source_adapter:Type[BaseSourceAdapter]):
        """Runs all nessesary pre-activities and instanciates the sample method.

        Prepare will be called before primary query compile time, so it can be used
        to do any nessesary pre-compile activites (such as collecting a histogram from the relation).

        Args:
            relation: The :class:`Relation <snowshu.core.models.relation.Relation>` object to prepare.
            source_adapter: The :class:`source adapter <snowshu.adapters.source_adapters.base_source_adapter.BaseSourceAdapter>` instance to use for executing prepare queries. 
        """
        raise NotImplementedError()
