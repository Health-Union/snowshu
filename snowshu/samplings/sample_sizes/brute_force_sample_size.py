import math
from snowshu.core.samplings.bases.base_sample_size import BaseSampleSize

class BruteForceSampleSize(BaseSampleSize):
    """Implements a static percentage sample size.

    Args:
        percentage: The decimal representation of the desired sample size between 1 and 99% (0.01 to 0.99).
    """

    def __init__(self,
                 percentage:float):

        self._percentage=percentage

    @property
    def percentage(self)->float:
        return self._percentage

    @percentage.setter
    def percentage(self,val:float)->None:
        """validates percentage between 1 and 99% before setting."""
        if (0.01 <= val <= 0.99):
            self._percentage=val
        else:
            raise ValueError(f"Percentage must be between 0.01 and 0.99, is {val}")


    def size(self,population:int)->int:
        """Calculates the sample size for a given population size.

        Args:
            population: The count of records in the full population.
        Returns:
            The minimum whole number of elements for a sample size given the instance margin of error and confidence.
        """
        return math.ceil(population * self.percentage)

