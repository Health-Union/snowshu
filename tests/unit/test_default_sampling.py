import pytest
from snowshu.samplings import DefaultSampling

ONE_BILLION_ROWS=1e9
ONE_HUNDRED_THOUSAND_ROWS=1e5
def test_default_sampling_stock():
    default=DefaultSampling()
    default.population=ONE_BILLION_ROWS
    assert default.sample_method.rows == 4147

def test_default_sampling_fine():
    default=DefaultSampling(0.01,0.99)
    default.population=ONE_BILLION_ROWS
    assert default.sample_method.rows == 16588

def test_default_sampling_min():
    default=DefaultSampling(0.1,0.50)
    default.population=ONE_HUNDRED_THOUSAND_ROWS
    assert default.sample_method.rows == 1000

def test_default_sampling_override_min():
    default=DefaultSampling(0.1,0.50, 5000)
    default.population=ONE_HUNDRED_THOUSAND_ROWS
    assert default.sample_method.rows == 5000
