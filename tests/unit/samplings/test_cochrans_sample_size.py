import pytest
from snowshu.samplings.sample_sizes import CochransSampleSize

def test_cochrans():
    sample_sizer=CochransSampleSize(0.04,0.95)
    assert sample_sizer.size(100000000) == 601

def test_div_by_zero():
    sample_sizer=CochransSampleSize(0.04,0.95)
    assert sample_sizer.size(0) == 0

