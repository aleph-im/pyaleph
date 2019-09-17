from aleph.model.base import BaseClass
from pymongo import ASCENDING, DESCENDING, IndexModel
import logging
LOGGER = logging.getLogger('model.messages')


class PendingMessage(BaseClass):
    """ Those messages have been received but their
    content will be retrieved later. """
    COLLECTION = "pending_messages"

    INDEXES = [IndexModel([("message.item_hash", ASCENDING)]),
               IndexModel([("message.sender", ASCENDING)]),
               IndexModel([("message.item_type", ASCENDING)]),
               IndexModel([("source.chain_name", ASCENDING)]),
               IndexModel([("source.height", ASCENDING)]),
               IndexModel([("message.time", DESCENDING)]),
               IndexModel([("message.time", ASCENDING)])]

class PendingTX(BaseClass):
    """ Those tx have been received onchain, but their
    content will be processed later. """
    COLLECTION = "pending_txs"

    INDEXES = [IndexModel([("context.time", DESCENDING)])]
