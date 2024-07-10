"""
Global objects used by API endpoints are stored in the aiohttp app object.
This module provides an abstraction layer over the dictionary keys used to
address these objects.
"""

import logging
from typing import Optional, TypeVar, cast

import aio_pika.abc
from aiohttp import web
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory

APP_STATE_CONFIG = "config"
APP_STATE_MQ_CONN = "mq_conn"
APP_STATE_MQ_CHANNEL = "mq_channel"
# RabbitMQ channel dedicated to websocket operations.
# A yet to be understood issue causes the websocket channel to close unexpectedly.
# We use a dedicated channel to avoid propagation of the issue to other endpoints.
APP_STATE_MQ_WS_CHANNEL = "mq_ws_channel"
APP_STATE_NODE_CACHE = "node_cache"
APP_STATE_P2P_CLIENT = "p2p_client"
APP_STATE_SESSION_FACTORY = "session_factory"
APP_STATE_STORAGE_SERVICE = "storage_service"
APP_STATE_SIGNATURE_VERIFIER = "signature_verifier"

T = TypeVar("T")


def get_config_from_request(request: web.Request) -> Config:
    return cast(Config, request.app[APP_STATE_CONFIG])


def get_ipfs_service_from_request(request: web.Request) -> Optional[IpfsService]:
    config = get_config_from_request(request)

    if not config.ipfs.enabled.value:
        return None

    storage_service = get_storage_service_from_request(request)
    return storage_service.ipfs_service


def get_mq_conn_from_request(request: web.Request) -> aio_pika.abc.AbstractConnection:
    return cast(aio_pika.abc.AbstractConnection, request.app[APP_STATE_MQ_CONN])


async def _get_open_channel(
    request: web.Request, channel_name: str, logger: logging.Logger
) -> aio_pika.abc.AbstractChannel:
    channel = cast(aio_pika.abc.AbstractChannel, request.app[channel_name])
    if channel.is_closed:
        # This should not happen, but does happen in practice because of RabbitMQ
        # RPC timeouts. We need to figure out where this timeout comes from,
        # but reopening the channel is mandatory to keep the endpoints using the MQ
        # functional.
        logger.error("%s channel is closed, reopening it", channel_name)
        await channel.reopen()

    return channel


async def get_mq_channel_from_request(
    request: web.Request, logger: logging.Logger
) -> aio_pika.abc.AbstractChannel:
    """
    Gets the MQ channel from the app state and reopens it if needed.
    """

    return await _get_open_channel(
        request=request, channel_name=APP_STATE_MQ_CHANNEL, logger=logger
    )


async def get_mq_ws_channel_from_request(
    request: web.Request, logger: logging.Logger
) -> aio_pika.abc.AbstractChannel:
    """
    Gets the websocket MQ channel from the app state and reopens it if needed.
    """

    return await _get_open_channel(
        request=request, channel_name=APP_STATE_MQ_WS_CHANNEL, logger=logger
    )


def get_node_cache_from_request(request: web.Request) -> NodeCache:
    return cast(NodeCache, request.app[APP_STATE_NODE_CACHE])


def get_p2p_client_from_request(request: web.Request) -> AlephP2PServiceClient:
    return cast(AlephP2PServiceClient, request.app[APP_STATE_P2P_CLIENT])


def get_session_factory_from_request(request: web.Request) -> DbSessionFactory:
    return cast(DbSessionFactory, request.app[APP_STATE_SESSION_FACTORY])


def get_storage_service_from_request(request: web.Request) -> StorageService:
    return cast(StorageService, request.app[APP_STATE_STORAGE_SERVICE])


def get_signature_verifier_from_request(request: web.Request) -> SignatureVerifier:
    return cast(SignatureVerifier, request.app[APP_STATE_SIGNATURE_VERIFIER])
