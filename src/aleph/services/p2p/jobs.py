import asyncio
import logging
from typing import List, Optional

from configmanager import Config
from p2pclient import Client as P2PClient

from aleph.model.p2p import get_peers
from .http import api_get_request
from .peers import connect_peer
from .protocol import AlephProtocol

from dataclasses import dataclass


@dataclass
class PeerStatus:
    peer_uri: str
    is_online: bool
    version: Optional[str]


LOGGER = logging.getLogger("P2P.jobs")


async def reconnect_p2p_job(config: Config, p2p_client: P2PClient, streamer: Optional[AlephProtocol]) -> None:
    await asyncio.sleep(2)
    while True:
        try:
            peers = set(
                config.p2p.peers.value + [a async for a in get_peers(peer_type="P2P")]
            )
            for peer in peers:
                try:
                    await connect_peer(p2p_client=p2p_client, streamer=streamer, peer_maddr=peer)
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


async def tidy_http_peers_job(config: Config, api_servers: List[str]) -> None:
    """Check that HTTP peers are reachable, else remove them from the list"""
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    await asyncio.sleep(2)

    while True:
        jobs = []

        try:
            async for peer in get_peers(peer_type="HTTP"):
                if my_ip in peer:
                    continue

                jobs.append(check_peer(peer))
            peer_statuses = await asyncio.gather(*jobs)

            for peer_status in peer_statuses:
                peer_in_api_servers = peer_status.peer_uri in api_servers

                if peer_status.is_online:
                    if not peer_in_api_servers:
                        api_servers.append(peer_status.peer_uri)

                else:
                    if peer_in_api_servers:
                        api_servers.remove(peer_status.peer_uri)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.p2p.reconnect_delay.value)
