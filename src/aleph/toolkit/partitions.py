"""Helpers for monthly RANGE-partitioned tables on a TIMESTAMPTZ column.

Used by:
* the alembic migration that creates crn_metrics / ccn_metrics
* the metrics_partition cron job that maintains them (create next month,
  drop past-cutoff)

Keeping the naming and bounds logic in one place means migration-time
partitions and cron-created partitions follow identical conventions.
"""

import datetime as dt
from typing import List, Tuple


def month_floor(d: dt.datetime) -> dt.datetime:
    """First instant of d's month, UTC."""
    return dt.datetime(d.year, d.month, 1, tzinfo=dt.timezone.utc)


def add_months(d: dt.datetime, months: int) -> dt.datetime:
    """Shift d by N calendar months (positive or negative). Snaps to the
    first day of the resulting month."""
    total = d.month - 1 + months
    year = d.year + total // 12
    month = total % 12 + 1
    return dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)


def monthly_bounds(
    start: dt.datetime, end_exclusive: dt.datetime
) -> List[Tuple[dt.datetime, dt.datetime]]:
    """List of [lower, upper) month-aligned ranges from start to
    end_exclusive. Both arguments should already be at month
    boundaries."""
    bounds: List[Tuple[dt.datetime, dt.datetime]] = []
    cursor = start
    while cursor < end_exclusive:
        upper = add_months(cursor, 1)
        bounds.append((cursor, upper))
        cursor = upper
    return bounds


def partition_name(table: str, lower: dt.datetime) -> str:
    """Naming convention: <table>_YYYY_MM, e.g. crn_metrics_2026_05."""
    return f"{table}_{lower.strftime('%Y_%m')}"


def ts_literal(d: dt.datetime) -> str:
    """TIMESTAMPTZ literal suitable for inlining into DDL."""
    return d.strftime("%Y-%m-%d %H:%M:%S%z")
