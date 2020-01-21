import logging
import pytest
from snowshu.logger import Logger
from tests.common import rand_string
from testfixtures import LogCapture


LOG_LEVELS = ('CRITICAL', 'ERROR', 'WARNING', 'DEBUG', 'INFO', 'NOTSET',)


@pytest.fixture(autouse=True)
def temp_log(tmpdir):
    temp_log = tmpdir.mkdir("sub").join("snowshu.log")
    yield temp_log


def test_logger_log_level(temp_log):
    log_engine = Logger()
    log_engine.initialize_logger(log_file_location=temp_log.strpath)
    logger = log_engine.logger
    log_engine.set_log_level(logging.DEBUG)
    with LogCapture() as capture:
        ERROR = rand_string(10)
        INFO = rand_string(10)
        DEBUG = rand_string(10)
        WARNING = rand_string(10)
        logger.warning(WARNING)
        logger.error(ERROR)
        logger.info(INFO)
        logger.debug(DEBUG)
        capture.check(
            ('snowshu', 'WARNING', WARNING),
            ('snowshu', 'ERROR', ERROR),
            ('snowshu', 'INFO', INFO),
            ('snowshu', 'DEBUG', DEBUG),

        )


def test_logger_debug_log_level(temp_log):
    log_engine = Logger()
    log_engine.initialize_logger(log_file_location=temp_log.strpath)
    log_engine.set_log_level(logging.DEBUG)
    logger = log_engine.logger
    with LogCapture() as capture:
        ERROR = rand_string(10)
        INFO = rand_string(10)
        DEBUG = rand_string(10)
        WARNING = rand_string(10)
        logger.warning(WARNING)
        logger.error(ERROR)
        logger.info(INFO)
        logger.debug(DEBUG)
        capture.check(
            ('snowshu', 'WARNING', WARNING),
            ('snowshu', 'ERROR', ERROR),
            ('snowshu', 'INFO', INFO),
            ('snowshu', 'DEBUG', DEBUG),
        )


@pytest.mark.skip
def test_logger_always_logs_debug_to_file(temp_log):
    levels = ('WARNING', 'DEBUG', 'INFO', 'CRITICAL',)
    log_engine = Logger()
    log_engine.initialize_logger(log_file_location=temp_log.strpath)
    for level in LOG_LEVELS:
        log_engine.set_log_level(level)
        logger = log_engine.logger
        message = rand_string(10)
        logger.debug(message)
        with open(temp_log) as tmp:
            line = tmp.readlines()[-1]
            assert 'DEBUG' in line
            assert message in line
