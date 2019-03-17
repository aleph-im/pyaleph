from aleph.model.base import BaseClass, Index
import pymongo
import logging
LOGGER = logging.getLogger('model.posts')


class Message(BaseClass):
    COLLECTION = "messages"

    INDEXES = [  # Index("hash", unique=True),
               Index("item_hash"),  # Content IPFS hash
               Index("sender"),
               Index("content.address"),
               Index("content.ref"),
               Index("content.type"),
               Index("content.tags"),
               Index("content.time"),
               Index("time", pymongo.DESCENDING),
               Index("time", pymongo.ASCENDING),
               Index("chain", pymongo.ASCENDING),
               Index("confirmations.chain", pymongo.ASCENDING),
               Index("confirmations.height", pymongo.ASCENDING),
               Index("confirmations.height", pymongo.DESCENDING),
               Index("confirmed")]
