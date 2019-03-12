from aleph import model
from aleph.model.base import BaseClass, Index
import pymongo
import logging
LOGGER = logging.getLogger('model.posts')

class AggregateEntries(BaseClass):
    COLLECTION = "aggregate_entries"

    INDEXES = [Index("hash", unique=True), #IPFS hash
               Index("address"),
               Index("time", pymongo.DESCENDING),
               Index("time", pymongo.ASCENDING),
               Index("confirmation_height", pymongo.ASCENDING),
               Index("confirmation_height", pymongo.DESCENDING),
               Index("confirmed")]
