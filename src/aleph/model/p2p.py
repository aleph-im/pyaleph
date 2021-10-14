"""
"""

from datetime import datetime, timedelta

from pymongo import ASCENDING, DESCENDING, IndexModel

from aleph.types import Protocol
from aleph.model.base import BaseClass


class Peer(BaseClass):
    """Holds information about the chains state."""

    COLLECTION = "peers"

    INDEXES = [
        IndexModel([("type", ASCENDING)]),
        IndexModel([("address", ASCENDING)]),
        IndexModel([("last_seen", DESCENDING)]),
    ]


async def get_peers(peer_type=None, hours=2):
    """Returns current peers.
    TODO: handle the last seen, channel preferences, and better way of avoiding "bad contacts".
    NOTE: Currently used in jobs.
    """
    async for peer in Peer.collection.find(
        {
            "type": peer_type,
            "last_seen": {"$gt": datetime.now() - timedelta(hours=hours)},
        }
    ).sort([("last_seen", -1)]):
        yield peer["address"]


async def add_peer(address, peer_type, source: Protocol, sender=None):
    await Peer.collection.replace_one(
        {"address": address, "type": peer_type},
        {
            "address": address,
            "type": peer_type,
            "last_seen": datetime.now(),
            "sender": sender,
            "source": source.value,
        },
        upsert=True,
    )
