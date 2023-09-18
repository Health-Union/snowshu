from unittest import mock
import pytest

from snowshu.samplings.samplings import BruteForceSampling


@pytest.fixture()
def mock_args():
    mock_rel=mock.MagicMock()
    mock_source_adapter=mock.MagicMock()
    yield mock_rel,mock_source_adapter


ONE_BILLION_ROWS=1e9
ONE_HUNDRED_THOUSAND_ROWS=1e5
def test_brute_force_sampling_stock(mock_args):
    mock_args[0].population_size=ONE_BILLION_ROWS
    brute_force=BruteForceSampling()
    brute_force.prepare(*mock_args)

    assert brute_force.sample_method.rows == 1000000

def test_brute_force_sampling_fine(mock_args):
    # GIVEN: mock_args - mock for snowshu.core.models.relation.Relation
    #        2% rows to be sampled of ONE_HUNDRED_THOUSAND_ROWS
    #        the min_sample_size is 3000
    #        expectation - 1000000 rows to be sampled
    brute_force=BruteForceSampling(probability = 0.02, min_sample_size = 3000)
    mock_args[0].population_size=ONE_BILLION_ROWS
    brute_force.prepare(*mock_args)
    assert brute_force.sample_method.rows == 1000000

def test_brute_force_sampling_min(mock_args):
    # GIVEN: mock_args - mock for snowshu.core.models.relation.Relation
    #        20% rows to be sampled of ONE_HUNDRED_THOUSAND_ROWS
    #        the min_sample_size is 5000
    #        expectation - 20000 rows to be sampled
    brute_force=BruteForceSampling(probability = 0.2, min_sample_size = 20000)
    mock_args[0].population_size=ONE_HUNDRED_THOUSAND_ROWS
    brute_force.prepare(*mock_args)
    assert brute_force.sample_method.rows == 20000

def test_brute_force_sampling_override_min(mock_args):
    # GIVEN: mock_args - mock for snowshu.core.models.relation.Relation
    #        10% rows to be sampled of ONE_HUNDRED_THOUSAND_ROWS
    #        the min_sample_size is 15000
    #        expectation - 15000 rows to be sampled
    brute_force=BruteForceSampling(probability = 0.1,  min_sample_size = 15000)
    mock_args[0].population_size=ONE_HUNDRED_THOUSAND_ROWS
    brute_force.prepare(*mock_args)
    assert brute_force.sample_method.rows == 15000

def test_brute_force_sampling_max(mock_args):
    # GIVEN: mock_args - mock for snowshu.core.models.relation.Relation
    #        20% rows to be sampled of ONE_HUNDRED_THOUSAND_ROWS
    #        the max_allowed_rows is 100
    #        expectation - 100 rows to be sampled
    brute_force=BruteForceSampling(probability = 0.2, min_sample_size = 0.50, max_allowed_rows=100)
    mock_args[0].population_size=ONE_HUNDRED_THOUSAND_ROWS
    brute_force.prepare(*mock_args)
    assert brute_force.sample_method.rows == 100