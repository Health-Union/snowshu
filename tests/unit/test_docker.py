import pytest
import mock
from snowshu.core.docker import SnowShuDocker
from snowshu.adapters.target_adapters import PostgresAdapter

def test_makes_replica_name_safe():
    shdocker=SnowShuDocker()
    VALID_REP_NAMES=[ "Replica With Spaces",
                "replica-with-----dashes",
                "--replica-lead-dashes",
                "replica.with.periods",
                "1_Replica_____with___underscores"]

    INVALID_REP_NAMES=["invalid-©haracter","$uper.i!!eg@l","replica_i!!egal_char"]

    valid_result=[shdocker.sanitize_replica_name(rep) for rep in VALID_REP_NAMES]

    assert valid_result==[   "snowshu_replica__replica-with-spaces",
                            "snowshu_replica__replica-with-dashes",
                            "snowshu_replica__replica-lead-dashes",
                            "snowshu_replica__replica-with-periods",
                            "snowshu_replica__1-replica-with-underscores"]

    for rep in INVALID_REP_NAMES:
        with pytest.raises(ValueError):
            shdocker.sanitize_replica_name(rep)

def test_remounts_data_in_replica():
    container=mock.MagicMock()
    container.exec_run.return_value=(0,'',)
    shdocker=SnowShuDocker()
    assert shdocker._remount_replica_data(container,PostgresAdapter())
    assert [arg for arg in container.exec_run.call_args_list][0][0][0] == 'cp /var/lib/postgresql/data /snowshu_replica_data'

    


