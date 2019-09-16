"""
"""

from aleph.model.base import BaseClass
from pymongo import ASCENDING, DESCENDING, IndexModel
import logging
LOGGER = logging.getLogger('model.p2p')


class Chain(BaseClass):
    """Holds information about the chains state."""
    COLLECTION = "peers"

    INDEXES = [IndexModel([("address", ASCENDING)]),
               IndexModel([("last_seen", DESCENDING)])]
    
async def get_peers():
    """ Returns current peers.
    TODO: handle the last seen, channel preferences, and better way of avoiding "bad contacts".
    NOTE: Currently used in jobs.
    """
    async for peer in Chain.collection.find():
        yield peer['address']
