from snowshu.core.sampling import BaseSampling
from snowshu.samplings.sample_sizes import CochransSampleSize

class DefaultSampling(BaseSampling):
    """Basic sampling using :class:`Cochrans <snowshu.samplings.sample_sizes.cochrans_sample_size.CochransSampleSize>` theorum for sample size and :class:`Bernoulli <snowshu.samplings.sample_methods.bernoulli_sample_method.BernoulliSampleMethod>` sampling.
    
    This default sampling assumes high volitility in the population
    """ 
