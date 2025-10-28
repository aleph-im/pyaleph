import logging
from pathlib import Path

import sentry_sdk
from aiohttp import web
from configmanager import Config

import aleph.config
from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.connection import make_engine, make_session_factory
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.p2p import init_p2p_client
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.monitoring import setup_sentry
from aleph.web import create_aiohttp_app
from aleph.web.controllers.app_state_getters import (
    APP_STATE_CONFIG,
    APP_STATE_MQ_CHANNEL,
    APP_STATE_MQ_CONN,
    APP_STATE_MQ_WS_CHANNEL,
    APP_STATE_NODE_CACHE,
    APP_STATE_P2P_CLIENT,
    APP_STATE_SESSION_FACTORY,
    APP_STATE_SIGNATURE_VERIFIER,
    APP_STATE_STORAGE_SERVICE,
)


async def configure_aiohttp_app(
    config: Config,
) -> web.Application:
    with sentry_sdk.start_transaction(name="init-api-server"):
        p2p_client = await init_p2p_client(config, service_name="api-server-aiohttp")

        engine = make_engine(
            config,
            echo=config.logging.level.value == logging.DEBUG,
            application_name="aleph-api",
        )
        session_factory = make_session_factory(engine)

        node_cache = NodeCache(
            redis_host=config.redis.host.value,
            redis_port=config.redis.port.value,
            message_count_cache_ttl=config.perf.message_count_cache_ttl.value,
        )
        # TODO: find a way to close the node cache when exiting the API process, not closing it causes
        #       a warning.
        await node_cache.open()
        # TODO: same, find a way to call await ipfs_service.close() on shutdown
        ipfs_service = IpfsService.new(config)

        storage_service = StorageService(
            storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
            ipfs_service=ipfs_service,
            node_cache=node_cache,
        )
        signature_verifier = SignatureVerifier()

        app = create_aiohttp_app()

        # Reuse the connection of the P2P client to avoid opening two connections
        mq_conn = p2p_client.mq_client.connection
        # Channels are long-lived, so we create one at startup. Otherwise, we end up hitting
        # the channel limit if we create a channel for each operation.
        mq_channel = await mq_conn.channel()
        mq_ws_channel = await mq_conn.channel()

        app[APP_STATE_CONFIG] = config
        app[APP_STATE_P2P_CLIENT] = p2p_client
        app[APP_STATE_MQ_CONN] = mq_conn
        app[APP_STATE_MQ_CHANNEL] = mq_channel
        app[APP_STATE_MQ_WS_CHANNEL] = mq_ws_channel
        app[APP_STATE_NODE_CACHE] = node_cache
        app[APP_STATE_STORAGE_SERVICE] = storage_service
        app[APP_STATE_SESSION_FACTORY] = session_factory
        app[APP_STATE_SIGNATURE_VERIFIER] = signature_verifier

    return app


async def create_app() -> web.Application:
    config = aleph.config.app_config

    # TODO: make the config file path configurable
    config_file = Path.cwd() / "config.yml"
    config.yaml.load(str(config_file))

    logging.basicConfig(level=config.logging.level.value)

    if config.sentry.dsn.value:
        setup_sentry(config)

    return await configure_aiohttp_app(config=config)


if __name__ == "__main__":
    web.run_app(create_app(), host="127.0.0.1", port=8000)
