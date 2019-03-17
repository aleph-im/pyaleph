from aleph.model.base import BaseClass, Index
import pymongo
import logging
LOGGER = logging.getLogger('model.posts')


class Post(BaseClass):
    COLLECTION = "posts"

    INDEXES = [Index("hash", unique=True),  # IPFS hash
               Index("sender"),
               Index("time", pymongo.DESCENDING),
               Index("time", pymongo.ASCENDING),
               Index("confirmation_height", pymongo.ASCENDING),
               Index("confirmation_height", pymongo.DESCENDING),
               Index("confirmed")]
