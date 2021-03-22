import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import Optional

from coloredlogs import ColoredFormatter

from snowshu.formats import (LOGGING_CLI_FORMAT, LOGGING_CLI_WARNING_FORMAT,
                             LOGGING_DATE_FORMAT, LOGGING_FILE_FORMAT)

# Tone down snowflake.connector log noise by only outputting warnings and
# higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)


def duration(start: int) -> str:
    dur = round(time.time() - start, 2)
    if dur < 1:
        return "< 1 second"

    return f"{dur} seconds"


class Logger:
    """File log is ALWAYS debug, regardless of log level."""
    DEFAULT_LOG_FILE_LOCATION = os.path.abspath('./snowshu.log')
    file_handler: Optional[RotatingFileHandler] = None

    def __init__(self) -> None:
        self._logger = logging.getLogger('snowshu')

    def initialize_logger(
            self,
            log_file_location: Optional[str] = DEFAULT_LOG_FILE_LOCATION) -> None:
        self.file_handler = self._construct_file_handler(log_file_location)
        self.file_handler.setFormatter(self._construct_file_formatter())

        stream_handler = self._construct_stream_handler()
        stream_handler.setFormatter(self._construct_colored_formatter())
        stream_handler.addFilter(self._exclude_warning_filter)

        warning_handler = self._construct_stream_handler()
        warning_handler.setFormatter(self._construct_warning_formatter())
        warning_handler.addFilter(self._warning_only_filter)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(warning_handler)
        self.logger.addHandler(self.file_handler)

    def set_log_level(self, level: str) -> None:
        logging.getLogger().setLevel(level)
        for handler in self.logger.handlers:
            if handler != self.file_handler:
                handler.setLevel(level)

    @staticmethod
    def remove_all_handlers(logger: logging.Logger) -> None:
        logger.handlers = list()

    def log_retries(self, retry_state):
        """ Function for passing to tenacity.retry decorator. """
        self.logger.warning('Retrying %s: attempt %s ended with: %s',
                            retry_state.fn.__qualname__,
                            retry_state.attempt_number,
                            retry_state.outcome.exception())

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def log_file_location(self) -> str:
        return self.file_handler.baseFilename

    @log_file_location.setter
    def log_file_location(self, value: str) -> None:
        self.file_handler.baseFilename = value

    # Handlers
    @staticmethod
    def _construct_file_handler(
            log_file_location: str) -> RotatingFileHandler:
        file_handler = RotatingFileHandler(log_file_location,
                                           maxBytes=10485760,
                                           backupCount=5)
        file_handler.setLevel(logging.DEBUG)
        return file_handler

    @staticmethod
    def _construct_stream_handler() -> logging.StreamHandler:
        return logging.StreamHandler()

    # Formatters
    @staticmethod
    def _construct_file_formatter() -> logging.Formatter:
        return logging.Formatter(fmt=LOGGING_FILE_FORMAT,
                                 datefmt=LOGGING_DATE_FORMAT)

    def _construct_colored_formatter(self) -> ColoredFormatter:
        return ColoredFormatter(fmt=LOGGING_CLI_FORMAT,
                                datefmt=LOGGING_DATE_FORMAT,
                                level_styles=self._colored_log_level_styles()
                                )

    def _construct_warning_formatter(self) -> ColoredFormatter:
        return ColoredFormatter(fmt=LOGGING_CLI_WARNING_FORMAT,
                                datefmt=LOGGING_DATE_FORMAT,
                                level_styles=self._colored_log_level_styles()
                                )

    @staticmethod
    def _colored_log_level_styles() -> dict:
        return {'critical': {'color': 'red'},
                'debug': {},
                'error': {'color': 'red'},
                'info': {},
                'notice': {'color': 'magenta'},
                'success': {'color': 'green'},
                'warning': {'color': 'yellow'}
                }

    # Filters
    @staticmethod
    def _warning_only_filter(record):
        return record.levelno == logging.WARNING

    @staticmethod
    def _exclude_warning_filter(record):
        return record.levelno != logging.WARNING
