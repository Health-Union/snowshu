import os
import click
from pathlib import Path
import logging
from snowshu.logger import Logger
from shutil import which, copyfile
from snowshu.formats import DEFAULT_TAG_FORMAT
from datetime import datetime
from snowshu.core.trail_path import TrailPath
from snowshu.core.sample import SampleRunner



# Always check for docker 
NO_DOCKER='SnowShu requires Docker, \
but it does not look like Docker is installed on this machine. \
See docs for more information at \
https://bitbucket.org/healthunion/snowshu/src/master/README.md'

TRAIL_PATH_DEFAULT=os.path.join(os.getcwd(),'trail-path.yml')

    
@click.group()
@click.option('--debug','-d', is_flag=True, default=False, help="run commands in debug mode")
def cli(debug:bool):
    log_level = logging.DEBUG if debug else logging.INFO
    (logging.getLogger()).setLevel(log_level)
    logger=Logger().logger
    if not which('docker'):
        logger.warning(NO_DOCKER)



@cli.command()
@click.argument('path', default=os.getcwd(),type=click.Path(exists=True))
def init(path:click.Path)->None:
    """generates sample trail-path.yml and credentials.yml files in the current directory."""
    logger=Logger().logger
    templates=os.path.join(Path(__file__).parent.parent,'templates')
    def destination(filename):
        return os.path.join(path,filename)
    def source(filename):
        return os.path.join(templates,filename)

    CREDENTIALS='credentials.yml'
    TRAIL_PATH='trail-path.yml'

    if os.path.isfile(destination(CREDENTIALS)) or os.path.isfile(destination(TRAIL_PATH)):
        logger.error("cannot generate sample files, already exist in current directory.")
        sys.exit()
    try:
        copyfile(source(TRAIL_PATH),destination(TRAIL_PATH)) 
        copyfile(source(CREDENTIALS),destination(CREDENTIALS)) 
        logger.info(f"sample files created in directory {os.path.abspath(path)}")
    except Exception as e:
        logger.error(f"failed to generate sample files: {e}")
        raise e

@cli.command()
@click.option('--tag', default=datetime.utcnow().strftime(DEFAULT_TAG_FORMAT))
@click.option('--trail-path-file', type=click.Path(exists=True), default=TRAIL_PATH_DEFAULT, help="the Path, string or bytes object snowshu will use for your trail_path configuration file, default is ./trail-path.yml")
@click.option('--dry-run', is_flag=True, default=False, help="compiles and prints both source and target queries without executing them.")
def sample( tag:str,
            trail_path_file:click.Path,
            dry_run:bool):
    trail_path=TrailPath()
    trail_path.load_config(trail_path_file)
    
    run=SampleRunner()
    run.execute(tag=tag,
                trail_path=trail_path,
                dry_run=dry_run)


@cli.command()
@click.option('--trail-path-file', type=click.Path(exists=True), default=TRAIL_PATH_DEFAULT, help="where snowshu will look for your trail_path configuration file, default is ./trail-path.yml")
def analyze(trail_path_file:click.Path):
    trail_path=TrailPath()
    trail_path.load_config(trail_path_file)
    trail_path.analyze()
    trail_path.pretty_print_analysis()
