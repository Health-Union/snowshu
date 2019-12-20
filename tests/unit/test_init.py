import logging
from pathlib import Path
from mock import patch, MagicMock
from tests.common import rand_string
from click.testing import CliRunner
import pytest
import os
from datetime import datetime,timedelta
from snowshu.core import main
from snowshu.formats import DEFAULT_TAG_FORMAT


def test_init_cli_happy_path(tmpdir):
    runner = CliRunner()
    pathdir=tmpdir.mkdir(rand_string(10)).strpath
    result = runner.invoke(main.cli, ('init', pathdir))
    assert result.exit_code == 0
    assert os.path.isfile(os.path.join(pathdir,'replica.yml'))
    assert os.path.isfile(os.path.join(pathdir,'credentials.yml'))
    assert f"sample files created in directory {os.path.abspath(pathdir)}" in result.output

def test_init_cli_sad_path(tmpdir):
    """make sure init does not overwrite"""
    runner = CliRunner()
    pathdir=tmpdir.mkdir(rand_string(10)).strpath
    Path(os.path.join(pathdir,'replica.yml')).touch()
    result = runner.invoke(main.cli, ('init', pathdir))
    assert result.exit_code == 1


@pytest.fixture
def temporary_replica():
    localpath=os.path.join(os.getcwd(),'replica.yml')
    if not os.path.isfile(localpath):
        Path(localpath).touch();
        yield localpath
        os.remove(localpath)
    else:
        yield localpath

@patch('snowshu.core.main.Replica.run')
@patch('snowshu.core.main.Replica.load_config')
def test_sample_defaults(run,replica, temporary_replica):
    runner = CliRunner()
    EXPECTED_REPLICA_FILE=temporary_replica
    result= runner.invoke(main.cli, ('sample',))
    ACTUAL_REPLICA_FILE=replica.call_args_list[0][0][0]
    run_args=run.call_args_list[0][1]

    ACTUAL_TAG_AS_DATETIME=datetime.strptime(run_args['tag'],DEFAULT_TAG_FORMAT)
    assert ACTUAL_REPLICA_FILE==EXPECTED_REPLICA_FILE
    assert ACTUAL_TAG_AS_DATETIME.date() == datetime.now().date()
    
    
@patch('snowshu.core.main.Replica.load_config')
@patch('snowshu.core.main.SampleRunner.execute')
def test_sample_args_valid(execute, replica):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tempfile=Path('./test-file.yml')
        tempfile.touch()
        EXPECTED_REPLICA_FILE=tempfile.absolute()
        EXPECTED_TAG=rand_string(10)
        EXPECTED_DRY_RUN=True
        EXPECTED_DEBUG=True
        result = runner.invoke(main.cli, ('--debug',
                                          'sample',
                                          '--replica-file',EXPECTED_REPLICA_FILE,
                                          '--tag',EXPECTED_TAG,
                                          '--dry-run',))
        replica.assert_called_once_with(EXPECTED_REPLICA_FILE)
        args=execute.call_args_list[0][1]
        ACTUAL_TAG=args['tag']
        assert EXPECTED_TAG==ACTUAL_TAG
        ACTUAL_DRY_RUN=args['dry_run']
        assert EXPECTED_DRY_RUN==ACTUAL_DRY_RUN
        assert logging.getLogger().getEffectiveLevel() == logging.DEBUG



@patch('snowshu.core.main.Replica')
@patch('snowshu.core.main.SampleRunner.execute')
def test_analyze_does_all_but_run(execute,replica):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tempfile=Path('./replica.yml')
        tempfile.touch()
        REPLICA_FILE=tempfile.absolute()
        result = runner.invoke(main.cli, ('analyze','--replica-file',REPLICA_FILE))
        replica_methods=replica.mock_calls
        assert '().load_config' == replica_methods[1][0]
        assert '().analyze' == replica_methods[2][0]
        replica.assert_called_once()
        execute.assert_not_called()

