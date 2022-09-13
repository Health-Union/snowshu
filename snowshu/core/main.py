import logging
import os
from pathlib import Path
from shutil import copyfile, which

import click

from snowshu.configs import IS_IN_DOCKER, DEFAULT_RETRY_COUNT
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.core.replica.replica_manager import ReplicaManager
from snowshu.logger import Logger

# Always check for docker
NO_DOCKER = 'SnowShu requires Docker, \
but it does not look like Docker is installed on this machine.\n \
See docs for more information at \
https://bitbucket.org/healthunion/snowshu/src/master/README.md'

REPLICA_DEFAULT = os.path.join(os.getcwd(), 'replica.yml')


@click.group()
@click.option('-v', '--verbosity', count=True, 
              help='Verbosity option: -v for debug in core , -vv for debug in core and adapters')
@click.option('--debug-core', is_flag=True, default=False, help='Set log level to debug only in core')
@click.option('--debug-adapters', is_flag=True, default=False, help='Set log level to debug only in adapters')
@click.option('--debug', '-d', is_flag=True, default=False, help='Set log level to debug everywhere')
def cli(debug: bool, debug_core: bool, debug_adapters: bool, verbosity: int):
    """SnowShu is a sampling engine designed to support testing in data development."""
    log_engine = Logger()
    log_engine.initialize_logger()

    core_log_level, adapter_log_level = logging.INFO, logging.INFO

    if verbosity > 0:
        if verbosity == 1:
            core_log_level, adapter_log_level = logging.DEBUG, logging.INFO
        elif verbosity >= 2:
            core_log_level, adapter_log_level = logging.DEBUG, logging.DEBUG

    if debug_core:
        core_log_level = logging.DEBUG

    if debug_adapters:
        adapter_log_level = logging.DEBUG

    if debug:
        core_log_level, adapter_log_level = logging.DEBUG, logging.DEBUG

    log_engine.set_log_level(core_level=core_log_level,
                             adapter_level=adapter_log_level)

    logger = log_engine.logger
    if not which('docker') and not IS_IN_DOCKER:
        logger.warning(NO_DOCKER)


@cli.command()
@click.argument('path', default=os.getcwd(), type=click.Path(exists=True))
def init(path: click.Path) -> None:
    """generates sample replica.yml and credentials.yml files in the current
    directory.

    Args:
        path: The full or relative path to where the files should be generated, defaults to current dir.
    """

    logger = logging.getLogger(__name__)
    templates = os.path.join(Path(__file__).parent.parent, 'templates')

    def destination(filename):
        return os.path.join(path, filename)

    def source(filename):
        return os.path.join(templates, filename)

    CREDENTIALS = 'credentials.yml'     # noqa pep8: disable=N806
    REPLICA = 'replica.yml'     # noqa pep8: disable=N806

    if os.path.isfile(destination(CREDENTIALS)) or os.path.isfile(
            destination(REPLICA)):
        message = "cannot generate sample files, already exist in current directory."
        logger.error(message)
        raise ValueError(message)
    try:
        copyfile(source(REPLICA), destination(REPLICA))
        copyfile(source(CREDENTIALS), destination(CREDENTIALS))
        logger.info(
            f"sample files created in directory {os.path.abspath(path)}")
    except Exception as exc:
        logger.error(f"failed to generate sample files: {exc}")
        raise exc


@cli.command()
@click.option(
    '--replica-file',
    type=click.Path(
        exists=True),
    default=REPLICA_DEFAULT,
    help="the Path, string or bytes object snowshu will use for your replica \
          configuration file, default is ./replica.yml")
@click.option('--name',
              help="Overrides the replica name found in replica.yml")
@click.option(
    '--barf', '-b',
    is_flag=True,
    help="outputs the source query sql to a local folder snowshu_barf_output")
@click.option(
    '--incremental', '-i',
    help="creates relations and loads data only for new entries found in replica.yml, "
         "which are not already present in target replica image")
@click.option(
    '--retry-count', '-r',
    help="Overrides default retry count (default is 1)",
    default=DEFAULT_RETRY_COUNT
)
@click.option(
    '--architecture', '-arch',
    help="",
    default=None,
    multiple=True
)
def create(replica_file: click.Path,  # noqa pylint: disable=too-many-arguments
           name: str,
           barf: bool,
           incremental: str,
           retry_count: int,
           architecture):
    """Generate a new replica from a replica.yml file.
    """

    if architecture:
        # actually neccessary, list() does not work properly
        target_arch = [x for x in architecture] # noqa pylint: unnecessary-comprehension
    else:
        target_arch = None

    replica = ReplicaFactory()
    replica.load_config(replica_file, target_arch=target_arch)
    replica.incremental = incremental

    click.echo(replica.create(name=name, barf=barf, retry_count=retry_count))


@cli.command()
@click.option(
    '--replica-file',
    type=click.Path(
        exists=True),
    default=REPLICA_DEFAULT,
    help="where snowshu will look for your replica configuration file, default is ./replica.yml")
@click.option('--barf', '-b',
              is_flag=True,
              help="outputs the source query sql to a local folder snowshu_barf_output")
@click.option(
    '--retry-count', '-r',
    help="Overrides default retry count (default is 1)",
    default=DEFAULT_RETRY_COUNT
)
def analyze(replica_file: click.Path,
            barf: bool,
            retry_count: int):
    """Perform a "dry run" of the replica creation without actually executing, and return the expected results.
    """
    replica = ReplicaFactory()
    replica.load_config(replica_file)
    click.echo(replica.analyze(barf=barf, retry_count=retry_count))


@cli.command()
def list():     # noqa pylint: disable=redefined-builtin
    """List all the available SnowShu replicas found on this computer."""
    replica_manager = ReplicaManager()
    click.echo(replica_manager.list())


@cli.command()
@click.argument('replica')
def launch_docker_cmd(replica: str):
    """Return the docker command line string to start a given replica."""
    replica_manager = ReplicaManager()
    click.echo(replica_manager.launch_docker_command(replica))
