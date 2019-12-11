import logging
import pytest
from snowshu.logger import Logger
from tests.common import rand_string
from testfixtures import LogCapture


LOG_LEVELS=('CRITICAL','ERROR','WARNING','DEBUG','INFO','NOTSET',)

@pytest.fixture(autouse=True)
def temp_log(tmpdir):
    temp_log=tmpdir.mkdir("sub").join("snowshu.log")
    yield temp_log


def test_logger_default_log_level(temp_log):
    logger = Logger(log_file_location=temp_log.strpath).logger
    ## default for logging is warning
    logger.setLevel(logging.WARNING)
    with LogCapture() as capture:
        ERROR=rand_string(10)
        INFO=rand_string(10)
        DEBUG=rand_string(10)
        WARNING=rand_string(10)
        logger.warning(WARNING)
        logger.error(ERROR)
        logger.info(INFO)
        logger.info(DEBUG)
        capture.check(
            ('snowshu','WARNING',WARNING),
            ('snowshu','ERROR',ERROR),
            )

def test_logger_debug_log_level(temp_log):
    root_logger=logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    logger = Logger(log_file_location=temp_log.strpath).logger
    with LogCapture() as capture:
        ERROR=rand_string(10)
        INFO=rand_string(10)
        DEBUG=rand_string(10)
        WARNING=rand_string(10)
        logger.warning(WARNING)
        logger.error(ERROR)
        logger.info(INFO)
        logger.debug(DEBUG)
        capture.check(
            ('snowshu','WARNING',WARNING),
            ('snowshu','ERROR',ERROR),
            ('snowshu','INFO',INFO),
            ('snowshu','DEBUG',DEBUG),
            )

def test_logger_always_logs_debug_to_file(temp_log):
    levels=('WARNING','DEBUG','INFO','CRITICAL',)
    for level in LOG_LEVELS:
        root_logger=logging.getLogger()
        root_logger.setLevel(level)
        logger = Logger(log_file_location=temp_log.strpath).logger
        message=rand_string(10)
        logger.debug(message)
        with open(temp_log) as tmp:
            line=tmp.readlines()[-1]
            assert 'DEBUG' in line
            assert message in line
