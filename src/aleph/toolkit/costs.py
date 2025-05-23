import datetime as dt
from decimal import ROUND_FLOOR, Decimal
from typing import Optional

from aleph.db.models.messages import MessageDb
from aleph.toolkit.constants import (
    PRICE_PRECISION,
    STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT,
    STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP,
)
from aleph.toolkit.timestamp import timestamp_to_datetime


def format_cost(v: Decimal | str, p: int = PRICE_PRECISION) -> Decimal:
    return Decimal(v).quantize(Decimal(1) / Decimal(10**p), ROUND_FLOOR)


def format_cost_str(v: Decimal | str, p: int = PRICE_PRECISION) -> str:
    n = format_cost(v, p)
    return "{:.{p}f}".format(n, p=p)


def are_store_and_program_free(message: MessageDb) -> bool:
    height: Optional[int] = (
        message.confirmations[0].height if len(message.confirmations) > 0 else None
    )
    date: dt.datetime = message.time

    if height is not None:
        return height < STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT
    else:
        return date < timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP)
