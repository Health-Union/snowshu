class BaseSampleSize:
    """The base class for all sample size methods. 

    Should not be invoked directly.

    """

    @property
    def size(self)->int:
        """The sample size.

        Must return the sample size (integer count of elements) caluclated
        by the class instance.

        Returns:
            the number of elements

        """        
