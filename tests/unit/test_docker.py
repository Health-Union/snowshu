import pytest
import mock
from snowshu.core.docker import SnowShuDocker
from snowshu.adapters.target_adapters import PostgresAdapter

def test_makes_image_tag_name_safe():
    shdocker=SnowShuDocker()
    VALID_TAG_NAMES=[ "Replica With Spaces",
                "replica-with-----dashes",
                "--replica-lead-dashes",
                "replica.with.periods",
                "1_Replica_____with___underscores"]

    INVALID_TAG_NAMES=["invalid-©haracter","$uper.i!!eg@l","replica_i!!egal_char"]

    valid_result=[shdocker.sanitize_tag_name(tag) for tag in VALID_TAG_NAMES]

    assert valid_result==[   "replica-with-spaces",
                            "replica-with-dashes",
                            "replica-lead-dashes",
                            "replica-with-periods",
                            "1-replica-with-underscores"]

    for tag in INVALID_TAG_NAMES:
        with pytest.raises(ValueError):
            shdocker.sanitize_tag_name(tag)

def test_remounts_data_in_replica():
    container=mock.MagicMock()
    container.exec_run.return_value=(0,'',)
    shdocker=SnowShuDocker()
    assert shdocker._remount_replica_data(container,PostgresAdapter())
    assert [arg for arg in container.exec_run.call_args_list][0][0][0] == 'cp /var/lib/postgresql/data /snowshu_replica_data'



@pytest.mark.skip()
def test_labels_with_target_adapter():
    pass


