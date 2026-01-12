from enum import Enum, IntEnum


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1

    def to_sql(self) -> str:
        """Return the SQL ORDER BY direction string."""
        return "ASC" if self == SortOrder.ASCENDING else "DESC"


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


class SortByAggregate(str, Enum):
    """
    Determines by which field aggregates should be sorted.
    """

    CREATION_TIME = "creation_time"
    LAST_MODIFIED = "last_modified"
