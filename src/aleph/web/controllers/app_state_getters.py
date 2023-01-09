"""
Global objects used by API endpoints are stored in the aiohttp app object.
This module provides an abstraction layer over the dictionary keys used to
address these objects.
"""

from typing import Optional, cast, TypeVar

import aio_pika.abc
from aiohttp import web
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config

from aleph.services.ipfs import IpfsService
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory

APP_STATE_CONFIG = "config"
APP_STATE_EXTRA_CONFIG = "extra_config"
APP_STATE_MQ_CONN = "mq_conn"
APP_STATE_P2P_CLIENT = "p2p_client"
APP_STATE_SESSION_FACTORY = "session_factory"
APP_STATE_SHARED_STATS = "shared_stats"
APP_STATE_STORAGE_SERVICE = "storage_service"


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


def get_p2p_client_from_request(request: web.Request) -> AlephP2PServiceClient:
    return cast(AlephP2PServiceClient, request.app[APP_STATE_P2P_CLIENT])


def get_session_factory_from_request(request: web.Request) -> DbSessionFactory:
    return cast(DbSessionFactory, request.app[APP_STATE_SESSION_FACTORY])


def get_storage_service_from_request(request: web.Request) -> StorageService:
    return cast(StorageService, request.app[APP_STATE_STORAGE_SERVICE])
