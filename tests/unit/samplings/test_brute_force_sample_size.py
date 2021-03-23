
import pytest

from snowshu.samplings.sample_sizes import BruteForceSampleSize


def test_cochrans():
    sample_sizer=BruteForceSampleSize(0.10)
    assert sample_sizer.size(100000000) == 10000000

def test_div_by_zero():
    sample_sizer=BruteForceSampleSize(0.50)
    assert sample_sizer.size(0) == 0
