from typing import TYPE_CHECKING
from snowshu.configs import MAX_ALLOWED_ROWS

from snowshu.core.samplings.bases.base_sampling import BaseSampling
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from snowshu.samplings.sample_sizes import BruteForceSampleSize

if TYPE_CHECKING:
    from snowshu.core.models.relation import Relation
    from snowshu.adapters.source_adapters.base_source_adapter import BaseSourceAdapter


class BruteForceSampling(BaseSampling):
    """
    Heuristic sampling using raw % size for sample size and :class:`Bernoulli
    <snowshu.samplings.sample_methods.bernoulli_sample_method.BernoulliSampleMethod>` sampling.

    Args:
        probability: The % sample size desired in decimal format from 0.01 to 0.99. Default 10%.
        min_sample_size: The minimum number of records to retrieve from the population. Default 1000.
    """
    size: int

    def __init__(self,
                 probability: float = 0.10,
                 min_sample_size: int = 1000,
                 max_allowed_rows: int = MAX_ALLOWED_ROWS):
        self.min_sample_size = min_sample_size
        self.max_allowed_rows = max_allowed_rows
        self.sample_size_method = BruteForceSampleSize(probability)

    def prepare(self,
                relation: "Relation",
                source_adapter: "BaseSourceAdapter") -> None:
        """Runs all necessary pre-activities and instanciates the sample method.

        Prepare will be called before primary query compile time, so it can be used
        to do any necessary pre-compile activities (such as collecting a histogram from the relation).

        Args:
            relation: The :class:`Relation <snowshu.core.models.relation.Relation>` object to prepare.
            source_adapter: The :class:`source adapter 
                <snowshu.adapters.source_adapters.base_source_adapter.BaseSourceAdapter>` instance to
                use for executing prepare queries.
        """
        self.size = min(max(self.sample_size_method.size(
            relation.population_size),
            self.min_sample_size), self.max_allowed_rows)

        self.sample_method = BernoulliSampleMethod(self.size,
                                                   units='rows')
