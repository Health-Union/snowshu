import os
from enum import Enum
from pathlib import Path
from shutil import which
import platform

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
DOCKER_API_TIMEOUT = 600  # in seconds, default is 60 which causes issues
POSTGRES_IMAGE = 'postgres:12'
DEFAULT_TEMPORARY_DATABASE = 'SNOWSHU'


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


class Architecture(Enum):
    ARM64 = "arm64"
    AARCH64 = "aarch64"
    AMD64 = "amd64"
    X86_64 = "x86_64"
    UNKNOWN = ""

ARCH_MAP = {
    Architecture.ARM64: Architecture.ARM64,
    Architecture.AARCH64: Architecture.ARM64,
    Architecture.AMD64: Architecture.AMD64,
    Architecture.X86_64: Architecture.AMD64,
}

def _get_architecture() -> Architecture:
    """
    Returns the machine type. Architecture.UNKNOWN is returned if the value cannot be determined.
    """
    iso_arch = platform.machine().lower()
    for arch in Architecture:
        if arch.value in iso_arch:
            return ARCH_MAP[arch]
    raise ValueError(f"Unknown architecture: {iso_arch}")


LOCAL_ARCHITECTURE: Architecture = _get_architecture()
IS_IN_DOCKER = _is_in_docker()
DOCKER_SHARED_FOLDER_NAME = 'snowshu_replica_data_shared'
DOCKER_REPLICA_MOUNT_FOLDER = os.path.join(os.path.sep,
                                           DOCKER_WORKING_DIR,
                                           DOCKER_SHARED_FOLDER_NAME)

DOCKER_REPLICA_VOLUME = 'snowshu_container_share'
