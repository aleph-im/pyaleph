import logging
from typing import Coroutine, List, Optional

from configmanager import Config

from aleph.services.ipfs import IpfsService
from aleph.types.db_session import DbSessionFactory

from ..cache.node_cache import NodeCache
from .client import P2PGrpcClient
from .manager import initialize_host

LOGGER = logging.getLogger(__name__)


async def init_p2p_client(config: Config, service_name: str) -> P2PGrpcClient:
    # service_name was used for RabbitMQ queue naming by the old client;
    # kept in the signature to avoid churn at call sites.
    _ = service_name
    return await P2PGrpcClient.connect(
        host=config.p2p.daemon_host.value,
        port=config.p2p.control_port.value,
    )


async def try_init_p2p_client(
    config: Config, service_name: str
) -> Optional[P2PGrpcClient]:
    """
    Best-effort connection for processes where the P2P service is an
    optimization, not a requirement (content fetch jobs). Returns None if
    the service is unreachable instead of failing the process.
    """
    try:
        return await init_p2p_client(config, service_name)
    except Exception:
        LOGGER.warning(
            "Could not connect to the P2P service; peer fetch disabled for %s",
            service_name,
        )
        return None


async def init_p2p_tasks(
    config: Config,
    session_factory: DbSessionFactory,
    p2p_client: P2PGrpcClient,
    ipfs_service: IpfsService,
    node_cache: NodeCache,
    listen: bool = True,
) -> List[Coroutine]:
    return await initialize_host(
        config=config,
        session_factory=session_factory,
        p2p_client=p2p_client,
        ipfs_service=ipfs_service,
        node_cache=node_cache,
        host=config.p2p.daemon_host.value,
        port=config.p2p.port.value,
        listen=listen,
    )
