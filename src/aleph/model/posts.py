from aleph.model.base import BaseClass
from pymongo import ASCENDING, DESCENDING, IndexModel


class Post(BaseClass):
    COLLECTION = "posts"

    INDEXES = [IndexModel([("hash", ASCENDING)], unique=True),  # IPFS hash
               IndexModel([("sender", ASCENDING)]),
               IndexModel([("time", DESCENDING)]),
               IndexModel([("time", ASCENDING)]),
               IndexModel([("confirmation_height", ASCENDING)]),
               IndexModel([("confirmation_height", DESCENDING)]),
               IndexModel([("confirmed", ASCENDING)])]
