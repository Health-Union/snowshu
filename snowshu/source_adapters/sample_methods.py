from dataclasses import dataclass

@dataclass
class SampleType:
    """ represents a sample method. 
        required_params(dict) is in format param=type
    """
    name:str
    required_params:dict

    def __str__(self)->str:
        return self.name

STRATIFIED=SampleType('STRATIFIED',
                      dict(percentage="float", strata="str"))

BERNOULLI=SampleType('BERNOULLI',
                     dict(probability="float"))

SYSTEM=SampleType('SYSTEM',
                   dict(probability="float"))
