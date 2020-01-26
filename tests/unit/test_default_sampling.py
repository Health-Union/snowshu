import pytest
from snowshu.samplings import DefaultSampling
import mock

@pytest.fixture()
def mock_args():
    mock_rel=mock.MagicMock()
    mock_source_adapter=mock.MagicMock()
    yield mock_rel,mock_source_adapter


ONE_BILLION_ROWS=1e9
ONE_HUNDRED_THOUSAND_ROWS=1e5
def test_default_sampling_stock(mock_args):
    mock_args[0].population_size=ONE_BILLION_ROWS
    default=DefaultSampling()
    default.prepare(*mock_args)

    assert default.sample_method.rows == 4147

def test_default_sampling_fine(mock_args):
    default=DefaultSampling(0.01,0.99)
    mock_args[0].population_size=ONE_BILLION_ROWS
    default.prepare(*mock_args)
    assert default.sample_method.rows == 16588

def test_default_sampling_min(mock_args):
    default=DefaultSampling(0.1,0.50)
    mock_args[0].population_size=ONE_HUNDRED_THOUSAND_ROWS
    default.prepare(*mock_args)
    assert default.sample_method.rows == 1000

def test_default_sampling_override_min(mock_args):
    default=DefaultSampling(0.1,0.50, 5000)
    mock_args[0].population_size=ONE_HUNDRED_THOUSAND_ROWS
    default.prepare(*mock_args)
    assert default.sample_method.rows == 5000
