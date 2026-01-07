from enum import Enum, IntEnum


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1


class SortBy(str, Enum):
    TIME = "time"
    TX_TIME = "tx-time"


class SortByMessageType(str, Enum):
    """
    Determines by which message type DB requests should be sorted.
    """

    AGGREGATE = "aggregate"
    FORGET = "forget"
    INSTANCE = "instance"
    POST = "post"
    PROGRAM = "program"
    STORE = "store"
    TOTAL = "total"
