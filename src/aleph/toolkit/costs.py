import datetime as dt
from decimal import ROUND_FLOOR, Decimal
from typing import Optional

from aleph.db.models.messages import MessageDb
from aleph.toolkit.constants import (
    CREDIT_ONLY_CUTOFF_TIMESTAMP,
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


def is_credit_only_required(message: MessageDb) -> bool:
    """
    Check if a message requires credit-only payment.

    After the cutoff, all paid messages (STORE, INSTANCE, PROGRAM) must use
    credit payment (no holding tier). Free messages/features are not affected.

    Messages before the cutoff can still use holding tier payment.

    Note: We only use timestamp-based cutoff here (not block height) because
    the cutoff will be set to a future date. Message timestamps are validated
    during message processing to prevent faking.
    """
    return message.time >= timestamp_to_datetime(CREDIT_ONLY_CUTOFF_TIMESTAMP)
