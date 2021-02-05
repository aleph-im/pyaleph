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
from typing import List, Coroutine

import sentry_sdk
from configmanager import Config
from setproctitle import setproctitle

from aleph import model
from aleph.chains import connector_tasks
from aleph.cli.args import parse_args
from aleph.config import load_config, get_defaults
from aleph.jobs import start_jobs, prepare_loop, prepare_manager, messages_task_loop, txs_task_loop, \
    reconnect_ipfs_job
from aleph.network import listener_tasks
from aleph.services import p2p
from aleph.services.p2p.manager import generate_keypair
from aleph.web import app, init_cors

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"

LOGGER = logging.getLogger(__name__)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


async def run_server(host: str, port: int, shared_stats:dict, extra_web_config: dict):
    # These imports will run in different processes
    from aiohttp import web
    from aleph.web.controllers.listener import broadcast

    # Reconfigure logging in different process
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    LOGGER.debug("Initializing CORS")
    init_cors()

    LOGGER.debug("Setup of runner")

    app['extra_config'] = extra_web_config
    app['shared_stats'] = shared_stats

    runner = web.AppRunner(app)
    await runner.setup()

    LOGGER.debug("Starting site")
    site = web.TCPSite(runner, host, port)
    await site.start()

    LOGGER.debug("Running broadcast server")
    await broadcast()
    LOGGER.debug("Finished broadcast server")


def run_server_coroutine(config_values, shared_stats, enable_sentry: bool = True,
                         extra_web_config = {}):
    """Run the server coroutine in a synchronous way.
    Used as target of multiprocessing.Process.
    """
    config = unpack_config(config_values)
    host = config.p2p.host.value
    port = config.p2p.http_port.value
    manager = None
    idx = 3

    setproctitle(f'pyaleph-run_server_coroutine-{port}')
    if enable_sentry:
        sentry_sdk.init(
            dsn=config.sentry.dns.value,
            traces_sample_rate=config.sentry.traces_sample_rate,
            ignore_errors=[KeyboardInterrupt],
        )
    # Use a try-catch-capture_exception to work with multiprocessing, see
    # https://github.com/getsentry/raven-python/issues/1110
    try:
        loop, tasks = prepare_loop(config_values, manager, idx=idx)
        loop.run_until_complete(
            asyncio.gather(*tasks, run_server(host, port, shared_stats, extra_web_config)))
    except Exception as e:
        if enable_sentry:
            sentry_sdk.capture_exception(e)
            sentry_sdk.flush()
        raise


def start_server_coroutine(config_serialized, shared_stats, enable_sentry,
                           extra_web_config) -> Process:
    process = Process(
        target=run_server_coroutine,
        args=(
            config_serialized,
            shared_stats,
            enable_sentry,
            extra_web_config,
        )
    )
    process.start()
    return process


def unpack_config(config_serialized):
    config = Config(schema=get_defaults())
    config.load_values(config_serialized)
    return config


def start_messages_task_loop(config_serialized) -> Process:
    """Start the messages task loop."""
    process = Process(
        target=messages_task_loop,
        args=(config_serialized, None),
    )
    process.start()
    return process


def start_txs_task_loop(config_serialized) -> Process:
    """Start the messages task loop."""
    process = Process(
        target=txs_task_loop,
        args=(config_serialized, None),
    )
    process.start()
    return process


def run_reconnect_ipfs_job(config_serialized):
    setproctitle('pyaleph-reconnect_ipfs_job')
    config = unpack_config(config_serialized)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(reconnect_ipfs_job(config))


def start_reconnect_ipfs_job(config_serialized) -> Process:
    process = Process(
        target=run_reconnect_ipfs_job,
        args=(config_serialized, ),
    )
    process.start()
    return process


def run_p2p(config_serialized):
    """"""
    setproctitle('pyaleph-run_p2p')
    config = unpack_config(config_serialized)
    tasks: List[Coroutine] = []

    LOGGER.debug("Initializing p2p")
    loop = asyncio.get_event_loop()
    f = p2p.init_p2p(config)
    tasks += loop.run_until_complete(f)
    tasks += listener_tasks(config)
    loop.run_until_complete(asyncio.gather(*tasks))


def start_p2p_process(config_serialized) -> Process:
    """Start the p2p connection."""
    process = Process(
        target=run_p2p,
        args=(config_serialized, ),
    )
    process.start()
    return process


def run_connector_tasks(config_serialized, outgoing: bool):
    setproctitle('pyaleph-run_connector_tasks')
    config = unpack_config(config_serialized)

    tasks: List[Coroutine] = connector_tasks(config, outgoing=outgoing)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*tasks))


def start_connector_tasks(config_serialized, outgoing: bool) -> Process:
    """Start the chain connector tasks."""
    process = Process(
        target=run_connector_tasks,
        args=(config_serialized, outgoing),
    )
    process.start()
    return process


def setup(args, config):
    """Stateful configuration of the node.
    """
    setup_logging(args.loglevel)
    set_start_method('spawn')

    if args.generate_key:
        LOGGER.info("Generating a key pair")
        generate_keypair(args.print_key, args.key_path)
        sys.exit(0)

    if args.sentry_disabled:
        LOGGER.info("Sentry disabled by CLI arguments")
    elif config.sentry.dsn.value:
        sentry_sdk.init(
            dsn=config.sentry.dsn.value,
            traces_sample_rate=config.sentry.traces_sample_rate.value,
            ignore_errors=[KeyboardInterrupt],
        )
        LOGGER.info("Sentry enabled")

    LOGGER.debug("Initializing database")
    model.init_db(config, ensure_indexes=(not args.debug))
    LOGGER.info("Database initialized.")

    # filestore.init_store(config)
    # LOGGER.info("File store initalized.")
    app['config'] = config
    init_cors()  # FIXME: This is stateful and process-dependent


def main(args, config):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """

    # Serialized version to pass to other processes.
    config_values = config.dump_values()

    manager = None
    if config.storage.engine.value == 'rocksdb':
        # rocksdb doesn't support multiprocess/multithread
        manager = prepare_manager(config_values)

    with Manager() as shared_memory_manager:
        tasks: List[Coroutine] = []
        # This dictionary is shared between all the process so we can expose some internal stats
        # handle with care as it's shared between process.
        shared_stats = shared_memory_manager.dict()
        if not args.no_jobs:
            LOGGER.debug("Creating jobs")
            tasks += start_jobs(config, shared_stats=shared_stats,
                                manager=manager, use_processes=False)

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
        extra_web_config = {
            'public_adresses': p2p_manager.public_adresses
        }

        p1 = Process(target=run_server_coroutine, args=(
            config_values,
            config.p2p.host.value,
            config.p2p.http_port.value,
            manager and (manager._address, manager._authkey) or None,
            3,
            shared_stats,
            args.sentry_disabled is False and config.sentry.dsn.value,
            extra_web_config,

        ))
        p2 = Process(target=run_server_coroutine, args=(
            config_values,
            config.aleph.host.value,
            config.aleph.port.value,
            manager and (manager._address, manager._authkey) or None,
            4,
            shared_stats,
            args.sentry_disabled is False and config.sentry.dsn.value,
            extra_web_config
        ))
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
    """Entry point for console_scripts
    """
    args = parse_args(sys.argv[1:])
    config = load_config(args)
    setup(args, config)
    main(args, config)


if __name__ == "__main__":
    run()
