import sys
import os
import click
from pathlib import Path
import logging
from snowshu.logger import Logger
from shutil import which, copyfile
from snowshu.formats import DEFAULT_TAG_FORMAT
from snowshu.configs import IS_IN_DOCKER
from datetime import datetime
from snowshu.core.replica import ReplicaFactory



# Always check for docker 
NO_DOCKER='SnowShu requires Docker, \
but it does not look like Docker is installed on this machine.\n \
See docs for more information at \
https://bitbucket.org/healthunion/snowshu/src/master/README.md'

REPLICA_DEFAULT=os.path.join(os.getcwd(),'replica.yml')

    
@click.group()
@click.option('--debug','-d', is_flag=True, default=False, help="run commands in debug mode")
def cli(debug:bool):
    log_level = logging.DEBUG if debug else logging.INFO
    log_engine=Logger()
    log_engine.initialize_logger()
    log_engine.set_log_level(log_level)
    logger=log_engine.logger
    if not which('docker') and not IS_IN_DOCKER:
        logger.warning(NO_DOCKER)



@cli.command()
@click.argument('path', default=os.getcwd(),type=click.Path(exists=True))
def init(path:click.Path)->None:
    """generates sample replica.yml and credentials.yml files in the current directory."""
    logger=Logger().logger
    templates=os.path.join(Path(__file__).parent.parent,'templates')

    def destination(filename):
        return os.path.join(path,filename)
    def source(filename):
        return os.path.join(templates,filename)

    CREDENTIALS='credentials.yml'
    REPLICA='replica.yml'

    if os.path.isfile(destination(CREDENTIALS)) or os.path.isfile(destination(REPLICA)):
        message="cannot generate sample files, already exist in current directory."
        logger.error(message)
        raise ValueError(message)
    try:
        copyfile(source(REPLICA),destination(REPLICA)) 
        copyfile(source(CREDENTIALS),destination(CREDENTIALS)) 
        logger.info(f"sample files created in directory {os.path.abspath(path)}")
    except Exception as e:
        logger.error(f"failed to generate sample files: {e}")
        raise e

@cli.command()
@click.option('--tag', default=datetime.utcnow().strftime(DEFAULT_TAG_FORMAT), help="the image tag of the resulting replication image. Defaults to timestamp")
@click.option('--replica-file', type=click.Path(exists=True), default=REPLICA_DEFAULT, help="the Path, string or bytes object snowshu will use for your replica configuration file, default is ./replica.yml")
def run( tag:str,
            replica_file:click.Path):
    replica=ReplicaFactory()
    replica.load_config(replica_file)
    replica.run(tag)


@cli.command()
@click.option('--replica-file', type=click.Path(exists=True), default=REPLICA_DEFAULT, help="where snowshu will look for your replica configuration file, default is ./replica.yml")
def analyze(replica_file:click.Path):
    replica=ReplicaFactory()
    replica.load_config(replica_file)
    click.echo(replica.analyze())
