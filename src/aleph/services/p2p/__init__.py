from typing import Coroutine, List, Tuple

from aleph_p2p_client import AlephP2PServiceClient, make_p2p_service_client
from configmanager import Config

from aleph.services.ipfs import IpfsService
from .manager import initialize_host
from aleph.types.db_session import DbSessionFactory
from ..cache.node_cache import NodeCache


async def init_p2p_client(config: Config, service_name: str) -> AlephP2PServiceClient:
    p2p_client = await make_p2p_service_client(
        service_name=service_name,
        mq_host=config.p2p.mq_host.value,
        mq_port=config.rabbitmq.port.value,
        mq_username=config.rabbitmq.username.value,
        mq_password=config.rabbitmq.password.value,
        mq_pub_exchange_name=config.rabbitmq.pub_exchange.value,
        mq_sub_exchange_name=config.rabbitmq.sub_exchange.value,
        http_host=config.p2p.daemon_host.value,
        http_port=config.p2p.control_port.value,
    )

    return p2p_client


async def init_p2p(
    config: Config,
    session_factory: DbSessionFactory,
    service_name: str,
    ipfs_service: IpfsService,
    node_cache: NodeCache,
    listen: bool = True,
) -> Tuple[AlephP2PServiceClient, List[Coroutine]]:

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
