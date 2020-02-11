import math
from snowshu.core.samplings.bases.base_sample_size import BaseSampleSize
from scipy.stats import norm as normal

class CochransSampleSize(BaseSampleSize):
    """Implements Cochran's theorum for large population sampling.

    More information about Cochran's theorum available here https://en.wikipedia.org/wiki/Cochran%27s_theorem.

    Args:
        margin_of_error: The decimal allowed error value between 1 and 10% (0.01 to 0.1).
        confidence: The decimal representation of the desired confidence between 1 and 99% (0.01 to 0.99).
    """

    def __init__(self,
                 margin_of_error:float,
                 confidence:float):
        self.margin_of_error=margin_of_error
        self.confidence=confidence

    @property
    def margin_of_error(self)->float:
        return self._margin_of_error
          
    @property
    def confidence(self)->float:
        return self._confidence

    @margin_of_error.setter
    def margin_of_error(self,val:float)->None:
        """validates margin of error between 1 and 10% before setting."""
        if (0.01 <= val <= 0.1):
            self._margin_of_error=val
        else:
            raise ValueError(f"Margin of error must be between 0.01 and 0.1, is {val}")

    @confidence.setter
    def confidence(self,val:float)->None:
        """validates confidence between 1 and 99% before setting."""
        if (0.01 <= val <= 0.99):
            self._confidence=val
        else:
            raise ValueError(f"Confidence must be between 0.01 and 0.99, is {val}")

    def size(self,population:int)->int:
        """Calculates the sample size for a given population size.
        
        Uses Cochran's theorum to return minimum viable sample size (rounded up to the nearest integer).

        Args:
            population: The count of records in the full population.
        Returns:
            The minimum whole number of elements for a sample size given the instance margin of error and confidence.
        """
        if population < 1:
            return 0 

        probability=0.5 
        n_zero=(((self._get_alpha()**2)
               * probability
               * (1.0-probability))
               /
               (self.margin_of_error**2))
        n=n_zero
        ## adjust for smaller pops
        if population < 50000:
            n=n_zero/(1 + ((n_zero-1)/population))
        return math.ceil(n)

    def _get_alpha(self)->float:
        """Returns the z-score ingoring both tails.
        Returns:
            z-score decimal inside both tails.
        """
        inside=1.0-((1-self.confidence)/2)
        return normal.ppf(inside)
