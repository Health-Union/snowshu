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
    assert os.path.isfile(os.path.join(pathdir,'trail-path.yml'))
    assert os.path.isfile(os.path.join(pathdir,'credentials.yml'))
    assert f"sample files created in directory {os.path.abspath(pathdir)}" in result.output

def test_init_cli_sad_path(tmpdir):
    runner = CliRunner()
    pathdir=tmpdir.mkdir(rand_string(10)).strpath
    Path(os.path.join(pathdir,'trail-path.yml')).touch()
    result = runner.invoke(main.cli, ('init', pathdir))
    assert result.exit_code == 1


@pytest.fixture
def temporary_trail_path():
    localpath=os.path.join(os.getcwd(),'trail-path.yml')
    if not os.path.isfile(localpath):
        Path(localpath).touch();
        yield localpath
        os.remove(localpath)
    else:
        yield localpath

@patch('snowshu.core.main.TrailPath.load_config')
@patch('snowshu.core.main.SampleRunner.execute')
def test_sample_defaults(execute, trail_path, temporary_trail_path):
    runner = CliRunner()
    EXPECTED_TRAIL_PATH_FILE=temporary_trail_path
    EXPECTED_DRY_RUN=False
    result= runner.invoke(main.cli, ('sample',))
    ACTUAL_TRAIL_PATH_FILE=trail_path.call_args_list[0][0][0]
    run_args=execute.call_args_list[0][1]

    ACTUAL_TAG_AS_DATETIME=datetime.strptime(run_args['tag'],DEFAULT_TAG_FORMAT)
    ACTUAL_DRY_RUN=run_args['dry_run']
    assert ACTUAL_DRY_RUN==EXPECTED_DRY_RUN
    assert ACTUAL_TRAIL_PATH_FILE==EXPECTED_TRAIL_PATH_FILE
    assert ACTUAL_TAG_AS_DATETIME.date() == datetime.now().date()
    
    
@patch('snowshu.core.main.TrailPath.load_config')
@patch('snowshu.core.main.SampleRunner.execute')
def test_sample_args_valid(execute, trail_path):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tempfile=Path('./test-file.yml')
        tempfile.touch()
        EXPECTED_TRAIL_PATH_FILE=tempfile.absolute()
        EXPECTED_TAG=rand_string(10)
        EXPECTED_DRY_RUN=True
        EXPECTED_DEBUG=True
        result = runner.invoke(main.cli, ('--debug',
                                          'sample',
                                          '--trail-path-file',EXPECTED_TRAIL_PATH_FILE,
                                          '--tag',EXPECTED_TAG,
                                          '--dry-run',))
        trail_path.assert_called_once_with(EXPECTED_TRAIL_PATH_FILE)
        args=execute.call_args_list[0][1]
        ACTUAL_TAG=args['tag']
        assert EXPECTED_TAG==ACTUAL_TAG
        ACTUAL_DRY_RUN=args['dry_run']
        assert EXPECTED_DRY_RUN==ACTUAL_DRY_RUN
        assert logging.getLogger().getEffectiveLevel() == logging.DEBUG



@patch('snowshu.core.main.TrailPath')
@patch('snowshu.core.main.SampleRunner.execute')
def test_analyze_does_all_but_run(execute,trail_path):
    runner = CliRunner()
    with runner.isolated_filesystem():
        tempfile=Path('./trail-path.yml')
        tempfile.touch()
        TRAIL_PATH_FILE=tempfile.absolute()
        result = runner.invoke(main.cli, ('analyze','--trail-path-file',TRAIL_PATH_FILE))
        trail_path_methods=trail_path.mock_calls
        assert '().load_config' == trail_path_methods[1][0]
        assert '().analyze' == trail_path_methods[2][0]
        assert '().pretty_print_analysis' == trail_path_methods[3][0]
        trail_path.assert_called_once()
        execute.assert_not_called()

