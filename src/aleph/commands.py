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

import asyncio
import logging
import sys
from multiprocessing import Process, set_start_method, Manager
from typing import List, Coroutine, Dict, Optional

from configmanager import Config
from setproctitle import setproctitle

from aleph import model
from aleph.chains import connector_tasks
from aleph.cli.args import parse_args
from aleph.config import get_defaults
from aleph.jobs import start_jobs, prepare_loop, prepare_manager, DBManager
from aleph.network import listener_tasks
from aleph.services import p2p
from aleph.services.p2p.manager import generate_keypair
from aleph.web import app, init_cors


import sentry_sdk

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"

LOGGER = logging.getLogger(__name__)


def setup_logging(loglevel: int):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


async def run_server(host: str, port: int, shared_stats: dict, extra_web_config: dict):
    # These imports will run in different processes
    from aiohttp import web
    from aleph.web.controllers.listener import broadcast

    # Reconfigure logging in different process
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    LOGGER.debug("Initializing CORS")
    init_cors()

    LOGGER.debug("Setup of runner")

    app["extra_config"] = extra_web_config
    app["shared_stats"] = shared_stats

    runner = web.AppRunner(app)
    await runner.setup()

    LOGGER.debug("Starting site")
    site = web.TCPSite(runner, host, port)
    await site.start()

    LOGGER.debug("Running broadcast server")
    await broadcast()
    LOGGER.debug("Finished broadcast server")


def run_server_coroutine(
    config_values,
    host,
    port,
    manager,
    idx,
    shared_stats,
    enable_sentry: bool = True,
    extra_web_config: Dict=None,
):
    """Run the server coroutine in a synchronous way.
    Used as target of multiprocessing.Process.
    """
    setproctitle(f'pyaleph-run_server_coroutine-{port}')
    extra_web_config = extra_web_config or {}
    logging.basicConfig(level=logging.DEBUG,
                        filename=f'/tmp/run_server_coroutine-{port}.log')
    if enable_sentry:
        sentry_sdk.init(
            dsn=config_values["sentry"]["dsn"],
            traces_sample_rate=config_values["sentry"]["traces_sample_rate"],
            ignore_errors=[KeyboardInterrupt],
        )
    # Use a try-catch-capture_exception to work with multiprocessing, see
    # https://github.com/getsentry/raven-python/issues/1110
    try:
        loop, tasks = prepare_loop(config_values, manager, idx=idx)
        loop.run_until_complete(
            asyncio.gather(
                *tasks, run_server(host, port, shared_stats, extra_web_config)
            )
        )
    except Exception as e:
        if enable_sentry:
            sentry_sdk.capture_exception(e)
            sentry_sdk.flush()
        raise


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """

    args = parse_args(args)
    setup_logging(args.loglevel)

    if args.generate_key:
        LOGGER.info("Generating a key pair")
        generate_keypair(args.print_key, args.key_path)
        return

    LOGGER.info("Loading configuration")
    config = Config(schema=get_defaults())
    app["config"] = config

    if args.config_file is not None:
        LOGGER.debug("Loading config file '%s'", args.config_file)
        app["config"].yaml.load(args.config_file)

    if (not config.p2p.key.value) and args.key_path:
        LOGGER.debug("Loading key pair from file")
        with open(args.key_path, "r") as key_file:
            config.p2p.key.value = key_file.read()

    if not config.p2p.key.value:
        LOGGER.critical("Node key cannot be empty")
        return

    if args.port:
        config.aleph.port.value = args.port
    if args.host:
        config.aleph.host.value = args.host

    if args.sentry_disabled:
        LOGGER.info("Sentry disabled by CLI arguments")
    elif app["config"].sentry.dsn.value:
        sentry_sdk.init(
            dsn=app["config"].sentry.dsn.value,
            traces_sample_rate=app["config"].sentry.traces_sample_rate.value,
            ignore_errors=[KeyboardInterrupt],
        )
        LOGGER.info("Sentry enabled")

    config_values = config.dump_values()

    LOGGER.debug("Initializing database")
    model.init_db(config, ensure_indexes=(not args.debug))
    LOGGER.info("Database initialized.")

    # filestore.init_store(config)
    # LOGGER.info("File store initalized.")
    init_cors()  # FIXME: This is stateful and process-dependent
    set_start_method("spawn")
    manager: Optional[DBManager] = None
    if config.storage.engine.value == "rocksdb":
        # rocksdb doesn't support multiprocess/multithread
        manager = prepare_manager(config_values)

    with Manager() as shared_memory_manager:
        tasks: List[Coroutine] = []
        # This dictionary is shared between all the process so we can expose some internal stats
        # handle with care as it's shared between process.
        shared_stats: Dict = shared_memory_manager.dict()
        if not args.no_jobs:
            LOGGER.debug("Creating jobs")
            tasks += start_jobs(
                config, shared_stats=shared_stats, manager=manager, use_processes=True
            )

        loop = asyncio.get_event_loop()

        # handler = app.make_handler(loop=loop)
        LOGGER.debug("Initializing p2p")
        f = p2p.init_p2p(config)
        p2p_tasks = loop.run_until_complete(f)
        tasks += p2p_tasks
        LOGGER.debug("Initialized p2p")

        LOGGER.debug("Initializing listeners")
        tasks += listener_tasks(config)
        tasks += connector_tasks(config, outgoing=(not args.no_commit))
        LOGGER.debug("Initialized listeners")

        # Need to be passed here otherwise it get lost in the fork
        from aleph.services.p2p import manager as p2p_manager

        extra_web_config = {"public_adresses": p2p_manager.public_adresses}

        p1 = Process(
            target=run_server_coroutine,
            args=(
                config_values,
                config.p2p.host.value,
                config.p2p.http_port.value,
                manager and (manager._address, manager._authkey) or None,
                3,
                shared_stats,
                args.sentry_disabled is False and app["config"].sentry.dsn.value,
                extra_web_config,
            ),
        )
        p2 = Process(
            target=run_server_coroutine,
            args=(
                config_values,
                config.aleph.host.value,
                config.aleph.port.value,
                manager and (manager._address, manager._authkey) or None,
                4,
                shared_stats,
                args.sentry_disabled is False and app["config"].sentry.dsn.value,
                extra_web_config,
            ),
        )
        p1.start()
        p2.start()
        LOGGER.debug("Started processes")

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
        LOGGER.debug("Running event loop")
        loop.run_until_complete(asyncio.gather(*tasks))


def run():
    """Entry point for console_scripts"""
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
