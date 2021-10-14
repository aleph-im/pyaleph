import asyncio
import json
import logging

from aleph.services.peers.common import ALIVE_TOPIC, IPFS_ALIVE_TOPIC
from aleph.services.ipfs.pubsub import pub as pub_ipfs

LOGGER = logging.getLogger("peers.publish")


async def publish_host(
    address, psub, interests=None, delay=120, peer_type="P2P", use_ipfs=True
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
                    pub_ipfs(IPFS_ALIVE_TOPIC, msg.decode("utf-8")), 1
                )
        except Exception:
            LOGGER.warning("Can't publish alive message on ipfs")

        try:
            LOGGER.debug("Publishing alive message on p2p pubsub")
            await asyncio.wait_for(psub.publish(ALIVE_TOPIC, msg), 1)
        except Exception:
            LOGGER.warning("Can't publish alive message on p2p")

        await asyncio.sleep(delay)
