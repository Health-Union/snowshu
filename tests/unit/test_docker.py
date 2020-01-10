import pytest
from snowshu.core.docker import SnowShuDocker

def test_makes_image_tag_name_safe():
    docker=SnowShuDocker()
    VALID_TAG_NAMES=[ "Replica With Spaces",
                "replica-with-----dashes",
                "--replica-lead-dashes",
                "replica.with.periods",
                "1_Replica_____with___underscores"]

    INVALID_TAG_NAMES=["invalid-©haracter","$uper.i!!eg@l","replica_i!!egal_char"]

    valid_result=[docker.sanitize_tag_name(tag) for tag in VALID_TAG_NAMES]

    assert valid_result==[   "replica-with-spaces",
                            "replica-with-dashes",
                            "replica-lead-dashes",
                            "replica-with-periods",
                            "1-replica-with-underscores"]

    for tag in INVALID_TAG_NAMES:
        with pytest.raises(ValueError):
            docker.sanitize_tag_name(tag)

@pytest.mark.skip
def test_remounts_data_in_replica():
    pass

@pytest.mark.skip()
def test_labels_with_target_adapter():
    pass


