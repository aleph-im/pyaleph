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
import uvloop
from configmanager import Config
from multiprocessing import Process, Manager

from aleph import __version__
from aleph.chains import start_connector
from aleph.web import app, init_cors, controllers
from aleph.config import get_defaults
from aleph.network import setup_listeners
from aleph.jobs import start_jobs, DBManager, prepare_loop, prepare_manager
from aleph import model
from aleph.services import p2p, filestore

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
    parser.add_argument('--no-commit', action="store_true", dest="no_commit",
                        default=False)
    parser.add_argument('--no-jobs', action="store_true", dest="no_jobs",
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
    

def run_server(config_values, host, port, manager, idx):
    from aiohttp import web
    loop = prepare_loop(config_values, manager, idx=idx)
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, host, port)
    loop.run_until_complete(site.start())
    loop.run_forever()

def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """

    # uvloop.install()
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
    
    filestore.init_store(config)
    LOGGER.info("File store initalized.")
    
    
    init_cors()
    manager = prepare_manager()
    if not args.no_jobs:
        start_jobs(config, manager=manager)

    loop = asyncio.get_event_loop()
    handler = app.make_handler(loop=loop)
    f = p2p.init_p2p(config)
    host = loop.run_until_complete(f)
    
    setup_listeners(config)
    start_connector(config, outgoing=(not args.no_commit))
    
    
    config_values = config.dump_values()
    p1 = Process(target=run_server, args=(config_values,
                                          config.p2p.host.value,
                                          config.p2p.http_port.value, 
                                          manager,
                                          3))
    p2 = Process(target=run_server, args=(config_values,
                                          config.aleph.host.value,
                                          config.aleph.port.value, 
                                          manager,
                                          4))
    p1.start()
    p2.start()
    
    # fp2p = loop.create_server(handler,
    #                           config.p2p.host.value,
    #                           config.p2p.http_port.value)
    # srvp2p = loop.run_until_complete(fp2p)
    # LOGGER.info('Serving on %s', srvp2p.sockets[0].getsockname())
    
    # f = loop.create_server(handler,
    #                        config.aleph.host.value,
    #                        config.aleph.port.value)
    # srv = loop.run_until_complete(f)
    # LOGGER.info('Serving on %s', srv.sockets[0].getsockname())
    loop.run_forever()


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
