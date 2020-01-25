from snowshu.core.samplings import BaseSampling
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from snowshu.samplings.sample_sizes import CochransSampleSize

class DefaultSampling(BaseSampling):
    """Basic sampling using :class:`Cochrans <snowshu.samplings.sample_sizes.cochrans_sample_size.CochransSampleSize>` theorum for sample size and :class:`Bernoulli <snowshu.samplings.sample_methods.bernoulli_sample_method.BernoulliSampleMethod>` sampling.
    
    This default sampling assumes high volitility in the population
    
    Args:
        margin_of_error: The acceptable error % expressed in a decimal from 0.01 to 0.10 (1% to 10%). Default 0.02 (2%). `https://en.wikipedia.org/wiki/Margin_of_error`
        confidence: The confidence interval to be observed for the sample expressed in a decimal from 0.01 to 0.99 (1% to 99%). Default 0.99 (99%). `http://www.stat.yale.edu/Courses/1997-98/101/confint.htm`
        min_sample_size: The minimum number of records to retrieve from the population. Default 1000.
    """ 
    def __init__(self,
                 margin_of_error:float=0.02,
                 confidence:float=0.99,
                 min_sample_size:int=1000):
        self.sample_size_method=CochransSampleSize(margin_of_error,
                                                   confidence)
        
    @property
    def population(self)->int:
        return self._population

    @population.setter
    def population(self,val)->None:
        self._population=math.ceil(val)
        self.sample_method=BernoulliSampleMethod(self.sample_size_method.size(self.population),
                           units='rows')
