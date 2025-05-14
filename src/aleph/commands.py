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
import os
import sys
from multiprocessing import set_start_method
from typing import Coroutine, List

import alembic.command
import alembic.config
import sentry_sdk
from configmanager import Config

import aleph.config
from aleph.chains.chain_data_service import ChainDataService, PendingTxPublisher
from aleph.chains.connector import ChainConnector
from aleph.cli.args import parse_args
from aleph.db.connection import make_db_url, make_engine, make_session_factory
from aleph.exceptions import InvalidConfigException, KeyNotFoundException
from aleph.jobs import start_jobs
from aleph.network import listener_tasks
from aleph.services import p2p
from aleph.services.cache.materialized_views import refresh_cache_materialized_views
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.keys import generate_keypair, save_keys
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.services.storage.garbage_collector import (
    GarbageCollector,
    garbage_collector_task,
)
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"

from aleph.toolkit.rabbitmq import make_mq_conn

LOGGER = logging.getLogger(__name__)


def run_db_migrations(config: Config):
    db_url = make_db_url(driver="psycopg2", config=config)
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic_cfg.attributes["configure_logger"] = False
    logging.getLogger("alembic").setLevel(logging.CRITICAL)
    alembic.command.upgrade(alembic_cfg, "head", tag=db_url)


async def init_node_cache(config: Config) -> NodeCache:
    node_cache = NodeCache(
        redis_host=config.redis.host.value, redis_port=config.redis.port.value
    )
    return node_cache


async def main(args: List[str]) -> None:
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """

    args = parse_args(args)
    setup_logging(args.loglevel)

    # Generate keys and exit
    if args.generate_keys:
        LOGGER.info("Generating a key pair")
        key_pair = generate_keypair(args.print_key)
        save_keys(key_pair, args.key_dir)
        if args.print_key:
            print(key_pair.private_key.impl.export_key().decode("utf-8"))  # type: ignore[attr-defined]

        return

    LOGGER.info("Loading configuration")
    config = aleph.config.app_config

    if args.config_file is not None:
        LOGGER.debug("Loading config file '%s'", args.config_file)
        config.yaml.load(args.config_file)

    # CLI config values override config file values
    config.logging.level.value = args.loglevel

    # Check for invalid/deprecated config
    if "protocol" in config.p2p.clients.value:
        msg = "The 'protocol' P2P config is not supported by the current version."
        LOGGER.error(msg)
        raise InvalidConfigException(msg)

    # We only check that the private key exists.
    private_key_file_path = os.path.join(args.key_dir, "node-secret.pkcs8.der")
    if not os.path.isfile(private_key_file_path):
        msg = f"Serialized node key ({private_key_file_path}) not found."
        LOGGER.critical(msg)
        raise KeyNotFoundException(msg)

    if args.sentry_disabled:
        LOGGER.info("Sentry disabled by CLI arguments")
    else:
        setup_sentry(config)
        LOGGER.info("Sentry enabled")

    LOGGER.info("Initializing database...")
    with sentry_sdk.start_transaction(name="run-migrations"):
        run_db_migrations(config)
    LOGGER.info("Database initialized.")

    engine = make_engine(
        config,
        echo=args.loglevel == logging.DEBUG,
        application_name="aleph-conn-manager",
    )
    session_factory = make_session_factory(engine)

    setup_logging(args.loglevel)

    mq_conn = await make_mq_conn(config)
    mq_channel = await mq_conn.channel()

    node_cache = await init_node_cache(config)
    async with node_cache, IpfsService.new(config) as ipfs_service:
        # Reset the cache
        await node_cache.reset()

        storage_service = StorageService(
            storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
            ipfs_service=ipfs_service,
            node_cache=node_cache,
        )
        garbage_collector = GarbageCollector(
            session_factory=session_factory, storage_service=storage_service
        )
        chain_data_service = ChainDataService(
            session_factory=session_factory,
            storage_service=storage_service,
        )
        pending_tx_publisher = await PendingTxPublisher.new(config=config)
        chain_connector = ChainConnector(
            session_factory=session_factory,
            pending_tx_publisher=pending_tx_publisher,
            chain_data_service=chain_data_service,
        )

        set_start_method("spawn")

        tasks: List[Coroutine] = []

        if not args.no_jobs:
            LOGGER.debug("Creating jobs")
            tasks += start_jobs(
                config=config,
                session_factory=session_factory,
                ipfs_service=ipfs_service,
                use_processes=True,
            )

        LOGGER.debug("Initializing p2p")
        p2p_client, p2p_tasks = await p2p.init_p2p(
            config=config,
            session_factory=session_factory,
            service_name="network-monitor",
            ipfs_service=ipfs_service,
            node_cache=node_cache,
        )
        tasks += p2p_tasks
        LOGGER.debug("Initialized p2p")

        LOGGER.debug("Initializing listeners")
        tasks += await listener_tasks(
            config=config,
            session_factory=session_factory,
            node_cache=node_cache,
            p2p_client=p2p_client,
            mq_channel=mq_channel,
        )
        tasks.append(chain_connector.chain_event_loop(config))
        LOGGER.debug("Initialized listeners")

        LOGGER.debug("Initializing cache tasks")
        tasks.append(refresh_cache_materialized_views(session_factory))
        LOGGER.debug("Initialized cache tasks")

        LOGGER.debug("Initializing garbage collector task")
        tasks.append(
            garbage_collector_task(config=config, garbage_collector=garbage_collector)
        )
        LOGGER.debug("Initialized garbage collector task")

        LOGGER.debug("Running event loop")
        await asyncio.gather(*tasks)


def run():
    """Entry point for console_scripts"""
    try:
        asyncio.run(main(sys.argv[1:]))
    except (KeyNotFoundException, InvalidConfigException):
        sys.exit(1)


if __name__ == "__main__":
    run()
