import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Sequence

import aiohttp
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config

from aleph.db.accessors.peers import get_all_addresses_by_peer_type
from aleph.db.models import PeerType
from aleph.types.db_session import AsyncDbSessionFactory

from ..cache.node_cache import NodeCache
from .http import api_get_request
from .peers import connect_peer


@dataclass
class PeerStatus:
    peer_uri: str
    is_online: bool
    version: Optional[str]


LOGGER = logging.getLogger("P2P.jobs")


async def reconnect_p2p_job(
    config: Config,
    session_factory: AsyncDbSessionFactory,
    p2p_client: AlephP2PServiceClient,
) -> None:
    await asyncio.sleep(2)

    while True:
        try:
            peers = set(config.p2p.peers.value)

            async with session_factory() as session:
                peers |= set(
                    await get_all_addresses_by_peer_type(
                        session=session, peer_type=PeerType.P2P
                    )
                )

            for peer in peers:
                try:
                    await connect_peer(p2p_client=p2p_client, peer_maddr=peer)
                except Exception:
                    LOGGER.debug("Can't reconnect to %s" % peer)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.p2p.reconnect_delay.value)


async def check_peer(
    session: aiohttp.ClientSession, peer_uri: str, timeout: int = 1
) -> PeerStatus:
    try:
        version_info = await api_get_request(
            session=session, base_uri=peer_uri, method="version", timeout=timeout
        )
        if version_info is not None:
            return PeerStatus(peer_uri=peer_uri, is_online=True, version=version_info)

    except Exception:
        LOGGER.exception("Can't contact peer %r" % peer_uri)

    return PeerStatus(peer_uri=peer_uri, is_online=False, version=None)


async def request_version(peers: Sequence[str], my_ip: str, timeout: int = 1):
    connector = aiohttp.TCPConnector(limit_per_host=5)
    timeout_conf = aiohttp.ClientTimeout(total=timeout)

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout_conf
    ) as session:
        jobs = []
        for peer in peers:
            if my_ip in peer:
                continue

            jobs.append(check_peer(session, peer))
        return await asyncio.gather(*jobs)


async def tidy_http_peers_job(
    config: Config, session_factory: AsyncDbSessionFactory, node_cache: NodeCache
) -> None:
    """Check that HTTP peers are reachable, else remove them from the list"""
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    await asyncio.sleep(2)

    while True:

        try:
            async with session_factory() as session:
                peers = await get_all_addresses_by_peer_type(
                    session=session, peer_type=PeerType.HTTP
                )

            peer_statuses = await request_version(peers=peers, my_ip=my_ip)

            for peer_status in peer_statuses:
                peer_in_api_servers = await node_cache.has_api_server(
                    peer_status.peer_uri
                )

                if peer_status.is_online:
                    if not peer_in_api_servers:
                        await node_cache.add_api_server(peer_status.peer_uri)

                else:
                    if peer_in_api_servers:
                        await node_cache.remove_api_server(peer_status.peer_uri)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.p2p.reconnect_delay.value)
