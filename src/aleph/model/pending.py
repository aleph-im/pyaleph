from aleph.model.base import BaseClass
from pymongo import ASCENDING, DESCENDING, IndexModel
import logging
LOGGER = logging.getLogger('model.messages')


class PendingMessage(BaseClass):
    """ Those messages have been received but their
    content will be retrieved later. """
    COLLECTION = "pending_messages"

    INDEXES = [IndexModel([("message.item_hash", ASCENDING)]),  # Content IPFS hash
               IndexModel([("message.sender", ASCENDING)]),
               IndexModel([("message.time", DESCENDING)]),
               IndexModel([("message.time", ASCENDING)]),
               IndexModel([("message.chain", ASCENDING)]),
               IndexModel([("source.chain_name", ASCENDING)]),
               IndexModel([("source.height", ASCENDING)]),]

class PendingTX(BaseClass):
    """ Those tx have been received onchain, but their
    content will be processed later. """
    COLLECTION = "pending_txs"

    INDEXES = [IndexModel([("context.publisher", ASCENDING)]),
               IndexModel([("context.time", DESCENDING)]),
               IndexModel([("context.chain_name", ASCENDING)]),
               IndexModel([("context.height", ASCENDING)]),
               IndexModel([("context.tx_hash", ASCENDING)]),]
