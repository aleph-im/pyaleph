import asyncio
import json
import logging
from typing import List, Optional

from aleph_p2p_client import AlephP2PServiceClient

from aleph.services.ipfs.pubsub import pub as pub_ipfs

LOGGER = logging.getLogger("peers.publish")


async def publish_host(
    address: str,
    p2p_client: AlephP2PServiceClient,
    p2p_alive_topic: str,
    ipfs_alive_topic: str,
    interests: Optional[List[str]] = None,
    delay: int = 120,
    peer_type: str = "P2P",
    use_ipfs: bool = True,
):
    """Publish our multiaddress regularly, saying we are alive."""
    await asyncio.sleep(2)
    from aleph import __version__

    msg = {
        "address": address,
        "interests": interests,
        "peer_type": peer_type,
        "version": __version__,
    }
    msg = json.dumps(msg).encode("utf-8")
    while True:
        try:
            if use_ipfs:
                LOGGER.debug("Publishing alive message on ipfs pubsub")
                await asyncio.wait_for(
                    pub_ipfs(ipfs_alive_topic, msg.decode("utf-8")), 1
                )
        except Exception:
            LOGGER.warning("Can't publish alive message on ipfs")

        try:
            LOGGER.debug("Publishing alive message on p2p pubsub")
            await asyncio.wait_for(
                p2p_client.publish(data=msg, topic=p2p_alive_topic), 10
            )
        except Exception:
            LOGGER.warning("Can't publish alive message on p2p")

        await asyncio.sleep(delay)
