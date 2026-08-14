import datetime as dt
from decimal import ROUND_FLOOR, Decimal
from typing import Optional

from aleph.db.models.messages import MessageDb
from aleph.toolkit.constants import (
    CREDIT_ONLY_CUTOFF_TIMESTAMP,
    HOLD_AND_STREAM_CUTOFF_TIMESTAMP,
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
    # Never trust the sender-supplied ``time``: use the observed (confirmation
    # or reception) time so a backdated message cannot claim the free legacy tier.
    date: dt.datetime = message.observed_time

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

    Note: We use the observed (confirmation or reception) time, never the
    sender-supplied ``time``, so a backdated message cannot dodge the
    credit-only requirement.
    """
    return message.observed_time >= timestamp_to_datetime(CREDIT_ONLY_CUTOFF_TIMESTAMP)


def is_hold_and_stream_deprecated(message: MessageDb) -> bool:
    """
    Check if hold and stream payment types are deprecated for this message.

    After the cutoff, new INSTANCE messages and persistent PROGRAM messages
    must use credit payment (hold and stream are no longer accepted).

    Uses the observed (confirmation or reception) time, never the
    sender-supplied ``time``, so the cutoff cannot be dodged by backdating.
    """
    return message.observed_time >= timestamp_to_datetime(
        HOLD_AND_STREAM_CUTOFF_TIMESTAMP
    )
