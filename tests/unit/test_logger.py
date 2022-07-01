import logging

import pytest
from testfixtures import LogCapture

from snowshu.logger import Logger
from tests.common import rand_string

LOG_LEVELS = (logging.WARNING, logging.DEBUG, logging.INFO, logging.CRITICAL, logging.ERROR, logging.NOTSET)


@pytest.fixture(autouse=True)
def temp_log(tmpdir):
    temp_log = tmpdir.mkdir("sub").join("snowshu.log")
    yield temp_log


def test_logger_log_level(temp_log):
    log_engine = Logger()
    log_engine.initialize_logger(log_file_location=temp_log.strpath)

    # test for both combinations of info / debug for all combos of core and adapter loggers
    for core_log_level in [logging.INFO, logging.DEBUG]:
        for adapter_log_level in [logging.INFO, logging.DEBUG]:
            log_engine.set_log_level(core_log_level, adapter_log_level)
            loggers = [
                {
                    'logger': logging.getLogger('snowshu'),
                    'name': 'snowshu',
                    'expected_level': core_log_level
                },
                {
                    'logger': logging.getLogger('snowshu.adapters'),
                    'name': 'snowshu.adapters',
                    'expected_level': adapter_log_level
                },
                {
                    'logger': logging.getLogger('snowshu.non_existing_module'),
                    'name': 'snowshu.non_existing_module',
                    'expected_level': core_log_level
                },
            ]

            for logger in loggers:

                ERROR = rand_string(10)
                INFO = rand_string(10)
                DEBUG = rand_string(10)
                WARNING = rand_string(10)

                with LogCapture() as capture:
                    logger['logger'].warning(WARNING)
                    logger['logger'].error(ERROR)
                    logger['logger'].info(INFO)
                    logger['logger'].debug(DEBUG)

                    if logger['expected_level'] == logging.DEBUG:
                        capture.check(
                            (logger['name'], 'WARNING', WARNING),
                            (logger['name'], 'ERROR', ERROR),
                            (logger['name'], 'INFO', INFO),
                            (logger['name'], 'DEBUG', DEBUG),
                        )
                    else:
                        capture.check(
                            (logger['name'], 'WARNING', WARNING),
                            (logger['name'], 'ERROR', ERROR),
                            (logger['name'], 'INFO', INFO),
                        )


def test_logger_debug_log_level(temp_log):
    log_engine = Logger()
    log_engine.initialize_logger(log_file_location=temp_log.strpath)
    log_engine.set_log_level(logging.DEBUG, logging.DEBUG)
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
    levels = (logging.WARNING, logging.DEBUG, logging.INFO, logging.CRITICAL)
    log_engine = Logger()
    log_engine.initialize_logger(log_file_location=temp_log.strpath)
    for level in LOG_LEVELS:
        log_engine.set_log_level(level, level)
        logger = log_engine.logger
        message = rand_string(10)
        logger.debug(message)
        with open(temp_log) as tmp:
            line = tmp.readlines()[-1]
            assert 'DEBUG' in line
            assert message in line
