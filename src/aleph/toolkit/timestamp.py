from typing import Optional, Union

import pytz
import datetime as dt


def timestamp_to_datetime(timestamp: float) -> dt.datetime:
    """
    Utility function that transforms a UNIX timestamp into a UTC-localized datetime
    object.
    """

    return pytz.utc.localize(dt.datetime.utcfromtimestamp(timestamp))


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
    Returns the current time as a UTC-localized datetime object.
    This differs from datetime.utcnow() because `utcnow()` is not localized.
    """
    return pytz.utc.localize(dt.datetime.utcnow())
