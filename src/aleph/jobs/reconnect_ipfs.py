"""
Job in charge of reconnecting to IPFS peers periodically.
"""

import asyncio
import datetime as dt
import logging

import aioipfs
from configmanager import Config

from aleph.db.accessors.peers import get_all_addresses_by_peer_type
from aleph.db.models import PeerType
from aleph.services.ipfs import IpfsService
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger("jobs.reconnect_ipfs")


async def reconnect_ipfs_job(
    config: Config, session_factory: DbSessionFactory, ipfs_service: IpfsService
):
    from aleph.services.utils import get_IP

    max_peer_age = dt.timedelta(seconds=config.p2p.max_peer_age.value)

    my_ip = await get_IP()
    await asyncio.sleep(2)
    while True:
        try:
            LOGGER.info("Reconnecting to peers")
            for peer in config.ipfs.peers.value:
                try:
                    ret = await ipfs_service.connect(peer)
                    if "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

            last_seen = dt.datetime.now(dt.timezone.utc) - max_peer_age
            with session_factory() as session:
                peers = get_all_addresses_by_peer_type(
                    session=session, peer_type=PeerType.IPFS, last_seen=last_seen
                )

            for peer in peers:
                if peer in config.ipfs.peers.value:
                    continue

                if my_ip in peer:
                    continue

                try:
                    ret = await ipfs_service.connect(peer)
                    if ret and "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.ipfs.reconnect_delay.value)
