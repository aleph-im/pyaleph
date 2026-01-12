from enum import Enum, IntEnum
from typing import Annotated, Any

from pydantic import BeforeValidator


def _parse_sort_order_for_metrics(value: Any) -> Any:
    """Parse ASC/DESC strings or 1/-1 integers to SortOrder values."""
    if isinstance(value, str):
        upper_value = value.upper()
        if upper_value in ("ASC", "ASCENDING"):
            return SortOrder.ASCENDING
        elif upper_value in ("DESC", "DESCENDING"):
            return SortOrder.DESCENDING
    return value


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1

    def to_sql(self) -> str:
        """Return the SQL ORDER BY direction string."""
        return "ASC" if self == SortOrder.ASCENDING else "DESC"


SortOrderForMetrics = Annotated[
    SortOrder, BeforeValidator(_parse_sort_order_for_metrics)
]


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
