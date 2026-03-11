import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional


def setup_logging(
    loglevel: int,
    filename: Optional[str] = None,
    max_log_file_size: Optional[int] = None,
) -> None:
    """
    Generic logging setup to be used by all processes.

    :param loglevel: Minimum loglevel for emitting messages.
    :param filename: Destination file for the logs, if specified. Defaults to stdout.
    :param max_log_file_size: Maximum size of the log file. Only applies if filename is specified.
    """

    # Some kwargs fiddling is required because basicConfig does not like it when stream
    # and handlers are specified at the same time.
    kwargs: Dict[str, Any]

    if filename:
        if not max_log_file_size:
            raise ValueError(
                "When logging to a log file, a max log file must be specified."
            )

        file_handler = RotatingFileHandler(
            filename, maxBytes=max_log_file_size, backupCount=4
        )
        # Also log to stderr so subprocess output is visible in docker compose logs
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.WARNING)
        kwargs = {"handlers": [file_handler, stderr_handler]}
    else:
        kwargs = {"stream": sys.stdout}

    logformat = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        **kwargs, level=loglevel, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
