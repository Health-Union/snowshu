from dataclasses import dataclass
from typing import Any

class SampleMethod:
    """ represents a sample method. 
        required_params(dict) is in format param=type
    """
    def name(self):
        raise NotImplementedError("SampleMethod instances must have a name.")

    def __repr__(self)->str:
        return f"<SampleMethod self.name>"

    def is_acceptable(self,value:Any)->bool:
        """returns if the given sample measure(s) is acceptable."""
        raise NotImplementedError("SampleMethod instances must test acceptability of results.")

class StratifiedSample(SampleMethod):

    name='STRATIFIED'
    def __init__(self,percentage:float,strata:str):
        self.percentage=percentage
        self.strata=strata      


class BernoulliSample(SampleMethod):
    name='BERNOULLI'
    def __init__(self,probability:float):
        self.probability=probability

    def is_acceptable(self,percent:float)->bool:
        """Accepts an actual percent and determines if within tolerances."""
        delta = abs(self.probability - (percent*100))\
                if percent < 1 else abs(self.probability - percent)
        return delta <=5 ## 5% diff ok? 

class SystemSample(SampleMethod):
    name='SYSTEM'

    def __init__(self,probability:float):
        self.probability:probability


def get_sample_method_from_kwargs(**kwargs)->SampleMethod:
    method=kwargs['sample_method']
    if method.upper() == 'BERNOULLI':
        return BernoulliSample(int(kwargs['probability']))
    else:
        raise ValueError(f'sample type {method} is unknown')
