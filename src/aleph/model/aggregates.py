from pymongo import ASCENDING, DESCENDING, IndexModel

from aleph.model.base import BaseClass


class AggregateEntries(BaseClass):
    COLLECTION = "aggregate_entries"

    INDEXES = [
        IndexModel([("hash", ASCENDING)], unique=True),  # IPFS hash
        IndexModel([("address", ASCENDING)]),
        IndexModel([("time", DESCENDING)]),
        IndexModel([("time", ASCENDING)]),
        IndexModel([("confirmation_height", ASCENDING)]),
        IndexModel([("confirmation_height", DESCENDING)]),
        IndexModel([("confirmed", ASCENDING)]),
    ]
