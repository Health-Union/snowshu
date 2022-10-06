import os
import re
from pathlib import Path
from shutil import which
import platform
from typing import List

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
DOCKER_WORKING_DIR = Path('/app').as_posix()


def _is_in_docker() -> bool:
    # running horizontal,
    # this should work for Unix AND Windows
    # https://stackoverflow.com/questions/36765138/bind-to-docker-socket-on-windows
    if os.path.exists(os.path.join(os.path.sep, 'var', 'run', 'docker.sock')
                      ) and not which('docker'):
        return True
    # running vertical (not recommended)
    try:
        with open('/proc/1/cgroup', 'rt') as ifh:  # noqa pylint: disable=unspecified-encoding
            # noqa pylint: disable=use-a-generator
            return any([indicator in line for line in ifh.readlines() for indicator in
                        ('docker', 'kubepods',)])
    except FileNotFoundError:
        return False


def _get_architecture() -> str:
    """
     Returns the machine type. An empty string is returned if the value cannot be determined.
    """
    iso_arch = platform.machine()
    arch = re.sub(r"(?:aarch64).*", "arm64", iso_arch) if 'aarch' in iso_arch.lower() else re.sub(r"(?:x86_64).*",
                                                                                                  "amd64", iso_arch)
    return arch


def _multiarch_list(local_arch) -> List[str]:
    if local_arch == 'amd64':
        return ['amd64', 'arm64']
    else:
        return ['arm64', 'amd64']

LOCAL_ARCHITECTURE = _get_architecture()
MULTIARCH_LIST = _multiarch_list(LOCAL_ARCHITECTURE)
IS_IN_DOCKER = _is_in_docker()
DOCKER_SHARED_FOLDER_NAME = 'snowshu_replica_data_shared'
DOCKER_REPLICA_MOUNT_FOLDER = os.path.join(os.path.sep, DOCKER_WORKING_DIR, DOCKER_SHARED_FOLDER_NAME)

DOCKER_REPLICA_VOLUME = 'snowshu_container_share'
