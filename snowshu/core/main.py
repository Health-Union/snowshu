import os
import logging
import importlib
from pathlib import Path
from shutil import copyfile, which

import click
import pendulum
import snowflake.connector

from snowshu.configs import IS_IN_DOCKER, DEFAULT_RETRY_COUNT, LOCAL_ARCHITECTURE
from snowshu.core.utils import get_multiarch_list
from snowshu.core.replica.replica_factory import ReplicaFactory
from snowshu.core.replica.replica_manager import ReplicaManager
from snowshu.core.utils import read_credentials_file
from snowshu.logger import Logger

# Always check for docker
NO_DOCKER = "SnowShu requires Docker, \
but it does not look like Docker is installed on this machine.\n \
See docs for more information at \
https://bitbucket.org/healthunion/snowshu/src/master/README.md"

REPLICA_DEFAULT = os.path.join(os.getcwd(), "replica.yml")


@click.group()
@click.option(
    "-v", "--verbosity", count=True, help="Verbosity option: -v for debug in core , -vv for debug in core and adapters"
)
@click.option("--debug-core", is_flag=True, default=False, help="Set log level to debug only in core")
@click.option("--debug-adapters", is_flag=True, default=False, help="Set log level to debug only in adapters")
@click.option("--debug", "-d", is_flag=True, default=False, help="Set log level to debug everywhere")
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

    log_engine.set_log_level(core_level=core_log_level, adapter_level=adapter_log_level)

    logger = log_engine.logger
    if not which("docker") and not IS_IN_DOCKER:
        logger.warning(NO_DOCKER)


@cli.command()
@click.argument("path", default=os.getcwd(), type=click.Path(exists=True))
def init(path: click.Path) -> None:
    """generates sample replica.yml and credentials.yml files in the current
    directory.

    Args:
        path: The full or relative path to where the files should be generated, defaults to current dir.
    """

    logger = logging.getLogger(__name__)
    templates = os.path.join(Path(__file__).parent.parent, "templates")

    def destination(filename):
        return os.path.join(path, filename)

    def source(filename):
        return os.path.join(templates, filename)

    CREDENTIALS = "credentials.yml"  # noqa: pylint: disable=invalid-name
    REPLICA = "replica.yml"  # noqa: pylint: disable=invalid-name

    if os.path.isfile(destination(CREDENTIALS)) or os.path.isfile(destination(REPLICA)):
        message = "cannot generate sample files, already exist in current directory."
        logger.error(message)
        raise ValueError(message)
    try:
        copyfile(source(REPLICA), destination(REPLICA))
        copyfile(source(CREDENTIALS), destination(CREDENTIALS))
        logger.info(f"sample files created in directory {os.path.abspath(path)}")
    except Exception as exc:
        logger.error(f"failed to generate sample files: {exc}")
        raise exc


@cli.command()
@click.option(
    "--replica-file",
    type=click.Path(exists=True),
    default=REPLICA_DEFAULT,
    help="the Path, string or bytes object snowshu will use for your replica \
          configuration file, default is ./replica.yml",
)
@click.option("--name", help="Overrides the replica name found in replica.yml")
@click.option("--barf", "-b", is_flag=True, help="outputs the source query sql to a local folder snowshu_barf_output")
@click.option(
    "--incremental",
    "-i",
    help="creates relations and loads data only for new entries found in replica.yml, "
    "which are not already present in target replica image",
)
@click.option("--retry-count", "-r", help="Overrides default retry count (default is 1)", default=DEFAULT_RETRY_COUNT)
@click.option(
    "--multiarch", "-m", help="Tells SnowShu to build replicas of both arm and amd architectures", is_flag=True
)
def create(
    replica_file: click.Path,  # noqa pylint: disable=too-many-arguments
    name: str,
    barf: bool,
    incremental: str,
    retry_count: int,
    multiarch,
):
    """Generate a new replica from a replica.yml file."""
    if multiarch:
        target_arch = get_multiarch_list(LOCAL_ARCHITECTURE)
    else:
        target_arch = [LOCAL_ARCHITECTURE.value]

    replica = ReplicaFactory()
    replica.load_config(replica_file, target_arch=target_arch)

    replica.check_adapter_support(replica, multiarch, "-m", "multiarch")

    replica.incremental = incremental
    click.echo(replica.create(name=name, barf=barf, retry_count=retry_count))


@cli.command()
@click.option(
    "--replica-file",
    type=click.Path(exists=True),
    default=REPLICA_DEFAULT,
    help="where snowshu will look for your replica configuration file, default is ./replica.yml",
)
@click.option("--barf", "-b", is_flag=True, help="outputs the source query sql to a local folder snowshu_barf_output")
@click.option("--retry-count", "-r", help="Overrides default retry count (default is 1)", default=DEFAULT_RETRY_COUNT)
def analyze(replica_file: click.Path, barf: bool, retry_count: int):
    """Perform a "dry run" of the replica creation without actually executing, and return the expected results."""
    replica = ReplicaFactory()
    replica.load_config(replica_file, [LOCAL_ARCHITECTURE.value])
    click.echo(replica.analyze(barf=barf, retry_count=retry_count))


@cli.command()
def list():  # noqa pylint: disable=redefined-builtin
    """List all the available SnowShu replicas found on this computer."""
    replica_manager = ReplicaManager()
    click.echo(replica_manager.list())


@cli.command()
@click.argument("replica")
def launch_docker_cmd(replica: str):
    """Return the docker command line string to start a given replica."""
    replica_manager = ReplicaManager()
    click.echo(replica_manager.launch_docker_command(replica))


@cli.group()
@click.option(
    "--type",
    "-t",
    help="The type of adapter to use for the operation.",
    type=click.Choice(["snowflake", "postgres"]),
    required=True,
)
@click.pass_context
def adapter(ctx, type):  # noqa pylint: disable=redefined-builtin
    """Adapter related commands."""
    ctx.ensure_object(dict)
    ctx.obj["TYPE"] = type


@adapter.command()
@click.option(
    "--credentials-file",
    default="./replicas/credentials.yml",
    type=click.Path(exists=True),
    help="The path to the credentials file.",
)
@click.option(
    "--prod-prefix",
    default="SNOWSHU_PROD",
    type=click.Choice(["SNOWSHU_PROD", "SNOWSHU_PRODUCTION", "SNOWSHU"], case_sensitive=False),
    help="The prefix to set for the prod replica objects.",
)
@click.option(
    "--replica-prefix",
    type=str,
    help="The prefix name of the staging replica objects.",
    required=True,
)
@click.pass_context
def promote(ctx, credentials_file: str, prod_prefix: str, replica_prefix: str):
    """Promote a replica to production."""
    if ctx.obj["TYPE"] != "snowflake":
        click.echo("Promote is only supported for Snowflake replicas.")
        return
    # Construct the module name dynamically
    module_name = f"snowshu.adapters.target_adapters.{ctx.obj['TYPE']}_adapter.utils"

    # Import the module
    utils_module = importlib.import_module(module_name)

    # Extract the required functions
    connect_to_database = utils_module.connect_to_database
    handle_existing_databases = utils_module.handle_existing_prod_databases
    handle_replica_databases = utils_module.handle_replica_databases

    credentials = read_credentials_file(credentials_file)["targets"][0]
    conn = connect_to_database(credentials)

    try:
        cursor = conn.cursor()
        current_date = pendulum.now()

        if handle_existing_databases(cursor, prod_prefix, replica_prefix, current_date):
            handle_replica_databases(cursor, replica_prefix, prod_prefix)
    except snowflake.connector.errors.Error as error:
        click.echo(f"Error during database operations: {error}")
    finally:
        cursor.close()
        conn.close()


@adapter.command()
@click.pass_context
def list_commands(ctx):
    """List available utilities for the selected type."""
    adapter_type = ctx.obj["TYPE"]
    if adapter_type == "snowflake":
        click.echo("Available Snowflake utilities:")
        click.echo("- promote: Promote a replica to production.")
        click.echo("- list_commands: List available utilities for the selected type.")
    else:
        click.echo(f"No utilities available for the selected type: {type}")
