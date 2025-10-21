import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Optional

from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config

from aleph.db.accessors.peers import get_all_addresses_by_peer_type
from aleph.db.models import PeerType
from aleph.types.db_session import DbSessionFactory
from .http import api_get_request
from .peers import connect_peer
from ..cache.node_cache import NodeCache


@dataclass
class PeerStatus:
    peer_uri: str
    is_online: bool
    version: Optional[str]


LOGGER = logging.getLogger("P2P.jobs")


async def reconnect_p2p_job(
    config: Config, session_factory: DbSessionFactory, p2p_client: AlephP2PServiceClient
) -> None:
    await asyncio.sleep(2)

    max_peer_age = dt.timedelta(seconds=config.p2p.max_peer_age.value)

    while True:
        try:
            peers = set(config.p2p.peers.value)

            last_seen = dt.datetime.now(dt.timezone.utc) - max_peer_age
            with session_factory() as session:
                peers |= set(
                    get_all_addresses_by_peer_type(
                        session=session, peer_type=PeerType.P2P, last_seen=last_seen
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


async def check_peer(peer_uri: str, timeout: int = 1) -> PeerStatus:
    try:
        version_info = await api_get_request(peer_uri, "version", timeout=timeout)
        if version_info is not None:
            return PeerStatus(peer_uri=peer_uri, is_online=True, version=version_info)

    except Exception:
        LOGGER.exception("Can't contact peer %r" % peer_uri)

    return PeerStatus(peer_uri=peer_uri, is_online=False, version=None)


async def tidy_http_peers_job(
    config: Config, session_factory: DbSessionFactory, node_cache: NodeCache
) -> None:
    """Check that HTTP peers are reachable, else remove them from the list"""
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    await asyncio.sleep(2)

    while True:
        jobs = []

        try:
            with session_factory() as session:
                peers = get_all_addresses_by_peer_type(
                    session=session, peer_type=PeerType.HTTP
                )

            for peer in peers:
                if my_ip in peer:
                    continue

                jobs.append(check_peer(peer))
            peer_statuses = await asyncio.gather(*jobs)

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
