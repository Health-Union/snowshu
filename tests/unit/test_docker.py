import pytest

from snowshu.core.docker import SnowShuDocker


def test_makes_replica_name_safe():
    shdocker = SnowShuDocker()
    VALID_REP_NAMES = ["Replica With Spaces",
                       "replica-with-----dashes",
                       "--replica-lead-dashes",
                       "replica.with.periods",
                       "1_Replica_____with___underscores"]

    INVALID_REP_NAMES = ["invalid-©haracter",
                         "$uper.i!!eg@l", "replica_i!!egal_char"]

    valid_result = [shdocker.sanitize_replica_name(
        rep) for rep in VALID_REP_NAMES]

    assert valid_result == ["snowshu_replica_replica-with-spaces",
                            "snowshu_replica_replica-with-dashes",
                            "snowshu_replica_replica-lead-dashes",
                            "snowshu_replica_replica-with-periods",
                            "snowshu_replica_1-replica-with-underscores"]

    for rep in INVALID_REP_NAMES:
        with pytest.raises(ValueError):
            shdocker.sanitize_replica_name(rep)
