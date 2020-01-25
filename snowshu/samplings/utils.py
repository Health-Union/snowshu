import snowshu.samplings as all_samplings
from typing import Union,Type
from snowshu.core.samplings import BaseSampling

def get_sampling_from_partial(partial:Union[dict,str])->Type[BaseSampling]:
    """takes a sampling config dict and returns an instance of :class:`BaseSampling <snowshu.core.samplings.base_sampling.BaseSampling>'

    This will accept either a string name of a sampling to invoke it with no params, or a dict to invoke with the params passed. 

    Args:
        partial: the portion of the configuration dict that creates the sampling.

    Example:
        Invoking with a partial of ``"default"`` will return an instance of 
        :class:`DefaultSampling <snowshu.samplings.default_sampling.DefaultSampling>` with all default values.
        Invoking with a partial of 
            ``{
                "default":{
                    "margin_of_error":0.03
                }
              }``
        will return an instance of 
        :class:`DefaultSampling <snowshu.samplings.default_sampling.DefaultSampling>` with margin_of_error set to 3%.

    Returns:
        The configured  :class:`DefaultSampling <snowshu.samplings.default_sampling.DefaultSampling>`.
    """
    def find_sampling_from_string(string:str)->Type[BaseSampling]:
        return all_samplings.__dict__[string.capitalize() + 'Sampling']
    try:
        return find_sampling_from_string(partial.keys()[0])(**partial)
    except AttributeError:
        return find_sampling_from_string(partial)()
