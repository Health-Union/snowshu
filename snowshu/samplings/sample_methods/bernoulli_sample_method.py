from typing import Union,Optional
from snowshu.core.samplings.bases.base_sample_method import BaseSampleMethod

class BernoulliSampleMethod(BaseSampleMethod):
    """Sample selection using the Bernoulli sampling method. `https://en.wikipedia.org/wiki/Bernoulli_trial`
    
    Args:
        value: the numeric sample size determinor, applied as units
        units: the unit of measure for the value param. Default is ``rows``

    Example:
        ``BernoulliSampleMethod(30)`` would give you a sample derived of aprox. 30 rows.
        ``BernoulliSampleMethod(0.3,units='probability') would give you a sample aprox. 30% of the population size.
    """
    name = 'BERNOULLI'

    def __init__(self,
                 value:Union[int,float],
                 units:Optional[str]='rows'):
        ok_units=('rows','probability',)
        assert units in ok_units
        self._rows,self._probability=[value if u == units else None for u in ok_units]
      
    @property
    def rows(self)->int:
        return self._rows

    @property
    def probability(self)->int:
        return self._probability
