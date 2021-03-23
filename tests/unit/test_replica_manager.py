import mock
import pytest

from snowshu.core.replica.replica_manager import ReplicaManager
from tests.common import rand_string


@mock.patch('snowshu.core.replica.replica_manager.SnowShuDocker.find_snowshu_images')
def tests_list_local(docker,mock_docker_image):
    docker.return_value=[mock_docker_image.get_image('snowshu-for_realz'),
                   mock_docker_image.get_image('also_snowshu',source_adapter='big_query')]
    rep_list=ReplicaManager.list()
    assert 'snowshu-for_realz' in rep_list

@mock.patch('snowshu.core.replica.replica_manager.SnowShuDocker.find_snowshu_images')
def test_launch_docker_cmd(docker,mock_docker_image):
    replica_name=rand_string(10)
    docker.return_value=[mock_docker_image.get_image(replica_name)]
    result=ReplicaManager.launch_docker_command(replica_name)
    cmd=f'docker run -d -p 9999:9999 --rm --name {replica_name} snowshu_replica_{replica_name}'
    assert result == cmd

@mock.patch('snowshu.core.replica.replica_manager.SnowShuDocker.find_snowshu_images')
def test_launch_docker_cmd_bad(docker,mock_docker_image):
    replica_name='does_not_exist'
    docker.return_value=[mock_docker_image.get_image(rand_string(10))]
    result=ReplicaManager.launch_docker_command(replica_name)

    assert result == f'No replica found for does_not_exist.'
