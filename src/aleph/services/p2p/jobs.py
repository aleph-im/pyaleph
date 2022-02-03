import asyncio
import logging
from typing import List, Optional

from configmanager import Config
from p2pclient import Client as P2PClient

from aleph.model.p2p import get_peers
from .http import api_get_request
from .peers import connect_peer
from .protocol import AlephProtocol

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


async def check_peer(peers: List[str], peer_uri: str, timeout: int = 1) -> None:
    try:
        version_info = await api_get_request(peer_uri, "version", timeout=timeout)
        if version_info is not None:
            peers.append(peer_uri)
    except Exception:
        LOGGER.exception("Can't contact peer %r" % peer_uri)


async def tidy_http_peers_job(config: Optional[Config] = None) -> None:
    """Check that HTTP peers are reachable, else remove them from the list"""
    from aleph.web import app
    from aleph.services.p2p import singleton
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    if config is None:
        config = app["config"]
    await asyncio.sleep(2)
    while True:
        try:
            peers: List[str] = list()
            jobs = list()
            async for peer in get_peers(peer_type="HTTP"):
                if my_ip in peer:
                    continue

                jobs.append(check_peer(peers, peer))
            await asyncio.gather(*jobs)
            singleton.api_servers = peers

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.p2p.reconnect_delay.value)
