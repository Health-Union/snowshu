from unittest import mock

import pytest

from snowshu.configs import _get_architecture

outputs_to_check = [
    (

        'x86_64 architecture',
        'x86_64',
        'amd64'
    ),
    (

        'ARM architecture(ISO)',
        'aarch64',
        'arm64'
    ),
    (

        'x86_64-v2 architecture',
        'x86_64-v2',
        'amd64'
    ),
    (

        'ARM architecture',
        'arm64',
        'arm64'
    ),
    (
        'AMD architecture',
        'amd64',
        'amd64'
    ),
]


@pytest.mark.parametrize('test_name, output_arch, expected', outputs_to_check, ids=[i[0] for i in outputs_to_check])
@mock.patch("platform.machine")
def test__get_architecture(arch_local, test_name, output_arch, expected):
    arch_local.return_value = output_arch
    arch = _get_architecture()
    assert arch == expected
