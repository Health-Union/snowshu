import pytest
from snowshu.core.samplings.utils import get_sampling_from_partial
from snowshu.samplings.samplings import DefaultSampling, BruteForceSampling


@pytest.mark.parametrize('sample_method, expected_sampling', [
    ('default', DefaultSampling),
    ('brute_force', BruteForceSampling)
])
def test_finds_bruite_force(sample_method, expected_sampling):
    """
        GIVEN: sample_method - the sampling literal
               expected_instance - the expected sampling class
    """
    assert isinstance(get_sampling_from_partial(sample_method), expected_sampling), f"The {expected_sampling.__name__} " \
                                                                                  f"test case failed."

