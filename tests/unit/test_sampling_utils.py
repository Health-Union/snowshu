from snowshu.samplings.utils import get_sampling_from_partial
from snowshu.samplings.samplings import DefaultSampling

def test_finds_default():
    assert isinstance(get_sampling_from_partial('default'),DefaultSampling)
