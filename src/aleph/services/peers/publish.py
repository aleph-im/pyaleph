import asyncio
import json
import logging
from typing import List, Optional

from aleph.services.p2p.client import P2PGrpcClient

LOGGER = logging.getLogger("peers.publish")


async def publish_host(
    address: str,
    p2p_client: P2PGrpcClient,
    p2p_alive_topic: str,
    interests: Optional[List[str]] = None,
    delay: int = 120,
    peer_type: str = "P2P",
):
    """
    Publish our multiaddress regularly, saying we are alive.

    Since release N+1 the alive message is only emitted on the P2P topic;
    it exists solely for pre-N nodes, which discover peers through it.
    Removed entirely in N+2.
    """
    await asyncio.sleep(2)
    from aleph.version import __version__

    msg = {
        "address": address,
        "interests": interests,
        "peer_type": peer_type,
        "version": __version__,
    }
    msg = json.dumps(msg).encode("utf-8")
    while True:
        try:
            LOGGER.debug("Publishing alive message on p2p pubsub")
            await asyncio.wait_for(
                p2p_client.publish(data=msg, topic=p2p_alive_topic), 10
            )
        except Exception:
            LOGGER.warning("Can't publish alive message on p2p")

        await asyncio.sleep(delay)
