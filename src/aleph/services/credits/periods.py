import datetime as dt
from typing import Dict

PERIOD = dt.timedelta(hours=1)

KIND_ORDER: Dict[str, int] = {
    "genesis": 0,
    "topup": 1,
    "transfer_in": 2,
    "transfer_out": 3,
    "execution": 4,
    "storage": 5,
    "hold_equivalent": 6,
}


def floor_hour(t: dt.datetime) -> dt.datetime:
    """Truncate a datetime to the start of its hour."""
    return t.replace(minute=0, second=0, microsecond=0)


def period_end(period_start: dt.datetime) -> dt.datetime:
    """Exclusive end of the hourly period starting at `period_start`."""
    return period_start + PERIOD
