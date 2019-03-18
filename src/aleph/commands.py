#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a skeleton file that can serve as a starting point for a Python
console script.

Then run `python setup.py install` which will install the command `pyaleph`
inside your current environment.
Besides console scripts, the header (i.e. until _logger...) of this file can
also be used as template for Python modules.
"""

import argparse
import sys
import logging
import asyncio
from configmanager import Config

from aleph import __version__
from aleph.chains import start_connector
from aleph.web import app, init_cors
from aleph.config import get_defaults
from aleph.network import setup_listeners
from aleph import model

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"

LOGGER = logging.getLogger(__name__)


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        prog="aleph",
        description="Aleph Network Node")
    parser.add_argument(
        '--version',
        action='version',
        version='pyaleph {ver}'.format(ver=__version__))
    parser.add_argument('-c', '--config', action="store", dest="config_file")
    parser.add_argument('-p', '--port', action="store", type=int, dest="port",
                        default=8080)
    parser.add_argument('--host', action="store", type=str, dest="host",
                        default="127.0.0.1")
    parser.add_argument('--debug', action="store_true", dest="debug",
                        default=False)
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    LOGGER.info("Starting up.")

    config = Config(schema=get_defaults())
    app['config'] = config
    app.config = config

    config.aleph.port.value = args.port
    config.aleph.host.value = args.host

    if args.config_file is not None:
        app['config'].yaml.load(args.config_file)

    model.init_db(config, ensure_indexes=(not args.debug))
    LOGGER.info("Database initialized.")

    init_cors()

    setup_listeners(config)
    start_connector(config)

    loop = asyncio.get_event_loop()
    handler = app.make_handler()
    f = loop.create_server(handler,
                           config.aleph.host.value,
                           config.aleph.port.value)
    srv = loop.run_until_complete(f)
    LOGGER.info('Serving on %s', srv.sockets[0].getsockname())
    loop.run_forever()


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
