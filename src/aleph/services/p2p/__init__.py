from typing import Coroutine, List, Tuple

from configmanager import Config

from aleph.services.ipfs import IpfsService
from aleph.types.db_session import DbSessionFactory

from ..cache.node_cache import NodeCache
from .client import P2PGrpcClient
from .manager import initialize_host


async def init_p2p_client(config: Config, service_name: str) -> P2PGrpcClient:
    # service_name was used for RabbitMQ queue naming by the old client;
    # kept in the signature to avoid churn at call sites.
    _ = service_name
    return await P2PGrpcClient.connect(
        host=config.p2p.daemon_host.value,
        port=config.p2p.control_port.value,
    )


async def init_p2p(
    config: Config,
    session_factory: DbSessionFactory,
    service_name: str,
    ipfs_service: IpfsService,
    node_cache: NodeCache,
    listen: bool = True,
) -> Tuple[P2PGrpcClient, List[Coroutine]]:

    p2p_client = await init_p2p_client(config, service_name)

    port = config.p2p.port.value
    tasks = await initialize_host(
        config=config,
        session_factory=session_factory,
        p2p_client=p2p_client,
        ipfs_service=ipfs_service,
        node_cache=node_cache,
        host=config.p2p.daemon_host.value,
        port=port,
        listen=listen,
    )

    return p2p_client, tasks
