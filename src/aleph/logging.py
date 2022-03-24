import logging
import sys
from typing import Any, Dict, Optional


def setup_logging(loglevel: int, filename: Optional[str] = None):
    """
    Generic logging setup to be used by all processes.

    :param loglevel: Minimum loglevel for emitting messages.
    :param filename: Destination file for the logs, if specified. Defaults to stdout.
    """

    # Some kwargs fiddling is required because basicConfig does not like it when stream
    # and filename are specified at the same time.
    kwargs: Dict[str, Any] = {"filename": filename} if filename else {"stream": sys.stdout}

    logformat = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        **kwargs, level=loglevel, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )
