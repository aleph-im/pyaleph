import datetime as dt
from typing import Optional, Union

import pytz


def timestamp_to_datetime(timestamp: float) -> dt.datetime:
    """
    Utility function that transforms a UNIX timestamp into a timezone-aware datetime object in UTC.
    """

    return dt.datetime.fromtimestamp(timestamp, tz=pytz.utc)


def coerce_to_datetime(
    datetime_or_timestamp: Optional[Union[float, dt.datetime]]
) -> Optional[dt.datetime]:
    # None for datetimes or 0 for timestamps results in returning None
    if datetime_or_timestamp is None or not datetime_or_timestamp:
        return None

    if isinstance(datetime_or_timestamp, dt.datetime):
        return datetime_or_timestamp

    return timestamp_to_datetime(datetime_or_timestamp)


def utc_now() -> dt.datetime:
    """
    Returns the current time as a timezone-aware datetime object in UTC.
    This differs from datetime.now() because `now()` returns a naive datetime object without timezone information.
    The returned object is timezone-aware, using UTC as the timezone.
    """

    return dt.datetime.now(tz=pytz.utc)
