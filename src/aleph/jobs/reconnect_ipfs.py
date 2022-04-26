"""
Job in charge of reconnecting to IPFS peers periodically.
"""

import asyncio
import logging

import aioipfs
from aleph.model.p2p import get_peers
from aleph.services.ipfs.common import connect_ipfs_peer

LOGGER = logging.getLogger("jobs.reconnect_ipfs")


async def reconnect_ipfs_job(config):
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    await asyncio.sleep(2)
    while True:
        try:
            LOGGER.info("Reconnecting to peers")
            for peer in config.ipfs.peers.value:
                try:
                    ret = await connect_ipfs_peer(peer)
                    if "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

            async for peer in get_peers(peer_type="IPFS"):
                if peer in config.ipfs.peers.value:
                    continue

                if my_ip in peer:
                    continue

                try:
                    ret = await connect_ipfs_peer(peer)
                    if ret and "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.ipfs.reconnect_delay.value)
