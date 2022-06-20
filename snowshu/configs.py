import os
from pathlib import Path
from shutil import which
from typing import Tuple, Optional

PACKAGE_ROOT = Path().parent.absolute()
MAX_ALLOWED_DATABASES = 2000
MAX_ALLOWED_ROWS = 1000000
DEFAULT_MAX_NUMBER_OF_OUTLIERS = 100
DEFAULT_PRESERVE_CASE = False
DEFAULT_INSERT_CHUNK_SIZE = 50000
DEFAULT_THREAD_COUNT = 4
DEFAULT_RETRY_COUNT = 1
DOCKER_NETWORK = 'snowshu'
DOCKER_TARGET_CONTAINER = 'snowshu_target'
DOCKER_REMOUNT_DIRECTORY = 'snowshu_replica_data'
DOCKER_TARGET_PORT = 9999
DOCKER_WORKING_DIR = Path('/tmp/app').as_posix()


def _is_in_docker() -> Tuple[bool, Optional[str]]:
    # running horizontal,
    # this should work for Unix AND Windows
    # https://stackoverflow.com/questions/36765138/bind-to-docker-socket-on-windows
    if os.path.exists(os.path.join(os.path.sep, 'var', 'run', 'docker.sock')
                      ) and not which('docker'):
        return True, os.uname().nodename
    # running vertical (not recommended)
    try:
        with open('/proc/1/cgroup', 'rt') as ifh: # noqa pylint: disable=unspecified-encoding
            return any([indicator in line for line in ifh.readlines() for indicator in ('docker', 'kubepods',)])  # noqa pylint: disable=use-a-generator
    except FileNotFoundError:
        return False, None


IS_IN_DOCKER, DOCKER_CONTAINER_NAME = _is_in_docker()
DOCKER_SHARED_FOLDER_NAME = 'snowshu_replica_data_shared'
LOCAL_REPLICA_MOUNT_FOLDER = os.path.join(os.path.sep, DOCKER_WORKING_DIR if IS_IN_DOCKER else PACKAGE_ROOT,
                                          DOCKER_SHARED_FOLDER_NAME)
DOCKER_REPLICA_MOUNT_FOLDER = os.path.join(os.path.sep, DOCKER_WORKING_DIR, DOCKER_SHARED_FOLDER_NAME)
