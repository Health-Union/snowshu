import logging

import pytest
from testfixtures import LogCapture

from snowshu.logger import Logger
from tests.common import rand_string

LOG_LEVELS = ('CRITICAL', 'ERROR', 'WARNING', 'DEBUG', 'INFO', 'NOTSET',)


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
            core_logger = logging.getLogger('snowshu')
            # this is temp until snowshu.logger can set both by itself
            core_logger.setLevel(core_log_level)

            adapter_logger = logging.getLogger('snowshu.adapters')
            # this is temp until snowshu.logger can set both by itself
            adapter_logger.setLevel(adapter_log_level)

            ERROR = rand_string(10)
            INFO = rand_string(10)
            DEBUG = rand_string(10)
            WARNING = rand_string(10)

            with LogCapture() as capture:
                core_logger.warning(WARNING)
                core_logger.error(ERROR)
                core_logger.info(INFO)
                core_logger.debug(DEBUG)

                if core_log_level == logging.DEBUG:
                    capture.check(
                        ('snowshu', 'WARNING', WARNING),
                        ('snowshu', 'ERROR', ERROR),
                        ('snowshu', 'INFO', INFO),
                        ('snowshu', 'DEBUG', DEBUG),
                    )
                else:
                    capture.check(
                        ('snowshu', 'WARNING', WARNING),
                        ('snowshu', 'ERROR', ERROR),
                        ('snowshu', 'INFO', INFO),
                    )
            
            with LogCapture() as capture:
                adapter_logger.warning(WARNING)
                adapter_logger.error(ERROR)
                adapter_logger.info(INFO)
                adapter_logger.debug(DEBUG)

                if adapter_log_level == logging.DEBUG:
                    capture.check(
                        ('snowshu.adapters', 'WARNING', WARNING),
                        ('snowshu.adapters', 'ERROR', ERROR),
                        ('snowshu.adapters', 'INFO', INFO),
                        ('snowshu.adapters', 'DEBUG', DEBUG),
                    )
                else:
                    capture.check(
                        ('snowshu.adapters', 'WARNING', WARNING),
                        ('snowshu.adapters', 'ERROR', ERROR),
                        ('snowshu.adapters', 'INFO', INFO),
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
