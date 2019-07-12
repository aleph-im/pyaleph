from aleph.model.base import BaseClass
from pymongo import ASCENDING, DESCENDING, IndexModel
import logging
LOGGER = logging.getLogger('model.messages')


class PendingMessage(BaseClass):
    """ Those messages have been received but their
    content can't be retrieved. """
    COLLECTION = "pending_messages"

    INDEXES = [IndexModel([("message.item_hash", ASCENDING)]),  # Content IPFS hash
               IndexModel([("message.sender", ASCENDING)]),
               IndexModel([("message.time", DESCENDING)]),
               IndexModel([("message.time", ASCENDING)]),
               IndexModel([("message.chain", ASCENDING)]),
               IndexModel([("source.chain_name", ASCENDING)]),
               IndexModel([("source.height", ASCENDING)]),]
