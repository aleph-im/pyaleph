from enum import Enum, IntEnum


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1


class SortBy(str, Enum):
    TIME = "time"
    TX_TIME = "tx-time"
