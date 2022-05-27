import os
from logging import DEBUG
from pathlib import Path
from unittest.mock import MagicMock, patch, ANY

import pytest
from click.testing import CliRunner

from snowshu.core import main
from snowshu.core.graph import SnowShuGraph
from snowshu.core.graph_set_runner import GraphSetRunner
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.logger import Logger
from snowshu.core.configuration_parser import Configuration
from tests.common import rand_string


def test_init_cli_happy_path(tmpdir):
    runner = CliRunner()
    pathdir = tmpdir.mkdir(rand_string(10)).strpath
    result = runner.invoke(main.cli, ('init', pathdir))
    assert result.exit_code == 0
    assert os.path.isfile(os.path.join(pathdir, 'replica.yml'))
    assert os.path.isfile(os.path.join(pathdir, 'credentials.yml'))
    assert f"sample files created in directory {os.path.abspath(pathdir)}" in result.output


def test_init_cli_sad_path(tmpdir):
    """make sure init does not overwrite"""
    runner = CliRunner()
    pathdir = tmpdir.mkdir(rand_string(10)).strpath
    Path(os.path.join(pathdir, 'replica.yml')).touch()
    result = runner.invoke(main.cli, ('init', pathdir))
    assert result.exit_code == 1


@pytest.fixture
def temporary_replica():
    localpath = os.path.join(os.getcwd(), 'replica.yml')
    if not os.path.isfile(localpath):
        Path(localpath).touch()
        yield localpath
        os.remove(localpath)
    else:
        yield localpath


@patch('snowshu.core.main.ReplicaFactory.create')
@patch('snowshu.core.main.ReplicaFactory.load_config')
def test_sample_defaults(load, create, temporary_replica):
    runner = CliRunner()
    EXPECTED_REPLICA_FILE = temporary_replica
    result = runner.invoke(main.cli, ('create',))
    ACTUAL_REPLICA_FILE = load.call_args_list[0][0][0]
    #run_args = create.call_args_list[0][0][0]   # wasn't used and broke down
    assert ACTUAL_REPLICA_FILE == EXPECTED_REPLICA_FILE


@patch('snowshu.core.main.ReplicaFactory.load_config')
@patch('snowshu.core.main.ReplicaFactory.create')
def test_sample_args_valid(run, replica):
    runner = CliRunner()
    with runner.isolated_filesystem():
        logger = Logger().logger
        tempfile = Path('./test-file.yml')
        tempfile.touch()
        EXPECTED_REPLICA_FILE = tempfile.absolute()
        EXPECTED_TAG = rand_string(10)
        EXPECTED_DEBUG = True
        result = runner.invoke(main.cli, ('--debug',
                                          'create',
                                          '--replica-file', EXPECTED_REPLICA_FILE,
                                          ))
        replica.assert_called_once_with(EXPECTED_REPLICA_FILE)
        assert logger.getEffectiveLevel() == DEBUG


@patch('snowshu.core.main.ReplicaFactory.target_adapter.create_relation')
@patch('snowshu.core.main.ReplicaFactory')
def test_analyze_does_all_but_run(replica, create_relation):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tempfile = Path('./replica.yml')
        tempfile.touch()
        REPLICA_FILE = tempfile.absolute()
        result = runner.invoke(
            main.cli, ('analyze', '--replica-file', REPLICA_FILE.absolute()))
        replica_methods = replica.mock_calls
        assert '().load_config' == replica_methods[1][0]
        assert '().analyze' == replica_methods[2][0]
        replica.assert_called_once()
        create_relation.assert_not_called()


@patch('snowshu.core.main.ReplicaFactory.load_config')
@patch('snowshu.core.main.ReplicaFactory.create')
def test_custom_retry_count_cli_input(create, load):  # noqa pylint: disable=unused-argument
    # test if CLI input is passed to replica.create
    runner = CliRunner()
    runner.invoke(main.cli, ('create'))
    create.assert_called_with(name=ANY, barf=ANY, retry_count=1)

    runner.invoke(main.cli, ('create --retry-count 5'))
    create.assert_called_with(name=ANY, barf=ANY, retry_count=5)

    runner.invoke(main.cli, ('create -r 50'))
    create.assert_called_with(name=ANY, barf=ANY, retry_count=50)


def test_custom_retry_count_passed_correctly_through_create():
    # test if replica.create passes retry count value to internal attribute retry_count
    with patch.object(ReplicaFactory, '_execute'):
        replica = ReplicaFactory()
        replica.create(name=None, barf=False, retry_count=5)
        assert replica.retry_count == 5


@patch('snowshu.core.replica.replica_factory.printable_result')
@patch('snowshu.core.replica.replica_factory.graph_to_result_list')
def test_custom_retry_count_passed_correctly_through_execute(graph_to_result_list, printable_result, stub_graph_set, stub_configs): # noqa pylint: disable=unused-argument

    # test if replica._execute passes retry count to GraphSetRunner.execute_graph_set
    def fake_build_graph(self, configs: Configuration) -> None: # noqa pylint: disable=unused-argument
        self.graph = stub_graph_set[0][-1]


    with patch.object(SnowShuGraph, 'build_graph', new=fake_build_graph),\
         patch.object(GraphSetRunner, 'execute_graph_set') as execute_graph_set_mock:
        for do_analyze in [True, False]:
            replica = ReplicaFactory()
            replica.retry_count = 5
            replica.run_analyze = do_analyze
            replica.load_config(stub_configs())

            # disable any target related functions
            replica.config.target_profile.adapter = MagicMock()

            replica._execute(name=None, barf=False)   # noqa pylint: disable=protected-access
            execute_graph_set_mock.assert_called_with(ANY,
                                                 ANY,
                                                 ANY,
                                                 threads=ANY,
                                                 retry_count=5,
                                                 analyze=do_analyze,
                                                 barf=ANY)
