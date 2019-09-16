"""
"""

from aleph.model.base import BaseClass
from pymongo import ASCENDING, DESCENDING, IndexModel
import logging
LOGGER = logging.getLogger('model.p2p')


class Chain(BaseClass):
    """Holds information about the chains state."""
    COLLECTION = "chains"

    INDEXES = [IndexModel([("address", ASCENDING)]),
               IndexModel([("last_seen", DESCENDING)])]
    
async def get_peers():
    for peer in await Chain.collection.find():
        yield peer
