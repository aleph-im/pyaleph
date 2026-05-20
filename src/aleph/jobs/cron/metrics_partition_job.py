"""Cron job that maintains monthly partitions of crn_metrics and
ccn_metrics.

Two responsibilities per run:

1. Pre-create the next ``LOOKAHEAD_MONTHS`` worth of monthly partitions
   if they don't already exist. This guarantees there's always a real
   partition ready for incoming scoring posts, so writes never have to
   fall back to the DEFAULT catch-all partition.

2. Detach + drop partitions whose upper bound is older than the
   retention cutoff (``RETENTION_MONTHS`` ago). DETACH first so the
   parent table only briefly holds an ACCESS EXCLUSIVE lock; the
   subsequent DROP only touches the (now-standalone) child table.

Both operations are idempotent. A run that finds the next partition
already present and nothing past the cutoff is a no-op.

The DEFAULT partition is left untouched. If it ever contains rows the
cron logs a warning (operational signal that the lookahead is too
short or that out-of-range data is arriving)."""

import datetime as dt
import logging
from typing import Iterable, List, Tuple

from sqlalchemy import text

from aleph.db.models.cron_jobs import CronJobDb
from aleph.jobs.cron.cron_job import BaseCronJob
from aleph.toolkit.partitions import (
    add_months,
    month_floor,
    monthly_bounds,
    partition_name,
    ts_literal,
)
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)

PARTITIONED_TABLES = ("crn_metrics", "ccn_metrics")


class MetricsPartitionCronJob(BaseCronJob):
    """Roll monthly partitions forward for the metrics tables.

    :param session_factory: DB session factory.
    :param retention_months: Drop partitions whose upper bound is older
        than ``now - retention_months``.
    :param lookahead_months: Ensure partitions exist up to and including
        ``now + lookahead_months``.
    """

    def __init__(
        self,
        session_factory: DbSessionFactory,
        retention_months: int,
        lookahead_months: int,
    ):
        self.session_factory = session_factory
        self.retention_months = retention_months
        self.lookahead_months = lookahead_months

    async def run(self, now: dt.datetime, job: CronJobDb) -> None:
        now_month = month_floor(now)
        cutoff = add_months(now_month, -self.retention_months)
        # Lookahead is inclusive: ensure partition for now_month + N
        # exists, so range becomes [..., now_month + N + 1).
        lookahead_upper = add_months(now_month, self.lookahead_months + 1)

        with self.session_factory() as session:
            for table in PARTITIONED_TABLES:
                self._ensure_partitions(session, table, now_month, lookahead_upper)
                self._drop_past_cutoff(session, table, cutoff)
                self._warn_if_default_has_rows(session, table)
            session.commit()

    @staticmethod
    def _ensure_partitions(
        session: DbSession,
        table: str,
        start: dt.datetime,
        end_exclusive: dt.datetime,
    ) -> None:
        """Create any missing monthly partitions in [start, end_exclusive)."""
        existing = _list_partitions(session, table)
        existing_names = {name for name, _, _ in existing}
        for lower, upper in monthly_bounds(start, end_exclusive):
            name = partition_name(table, lower)
            if name in existing_names:
                continue
            LOGGER.info(
                "Creating partition %s on %s for [%s, %s)",
                name,
                table,
                lower.isoformat(),
                upper.isoformat(),
            )
            session.execute(
                text(
                    f"CREATE TABLE {name} PARTITION OF {table} "
                    f"FOR VALUES FROM ('{ts_literal(lower)}') "
                    f"TO ('{ts_literal(upper)}')"
                )
            )

    @staticmethod
    def _drop_past_cutoff(session: DbSession, table: str, cutoff: dt.datetime) -> None:
        """DETACH + DROP partitions whose upper bound is <= cutoff.

        DETACH briefly takes ACCESS EXCLUSIVE on the parent, then the
        DROP only touches the now-standalone child. Metrics tables are
        not on a latency-sensitive read path so plain DETACH is fine;
        CONCURRENTLY would require autocommit, which the cron's
        transactional session doesn't offer."""
        for name, lower, upper in _list_partitions(session, table):
            if upper is None or lower is None:
                # The DEFAULT partition has no bounds. Skip.
                continue
            if upper <= cutoff:
                LOGGER.info(
                    "Dropping partition %s on %s (upper=%s <= cutoff=%s)",
                    name,
                    table,
                    upper.isoformat(),
                    cutoff.isoformat(),
                )
                session.execute(text(f"ALTER TABLE {table} DETACH PARTITION {name}"))
                session.execute(text(f"DROP TABLE {name}"))

    @staticmethod
    def _warn_if_default_has_rows(session: DbSession, table: str) -> None:
        default_name = f"{table}_default"
        result = session.execute(text(f"SELECT count(*) FROM {default_name}")).scalar()
        if result and result > 0:
            LOGGER.warning(
                "DEFAULT partition %s holds %s rows. Lookahead may be too "
                "short, or out-of-range timestamps are arriving.",
                default_name,
                result,
            )


def _list_partitions(
    session: DbSession, parent: str
) -> List[Tuple[str, dt.datetime, dt.datetime]]:
    """Return (child_name, lower_bound, upper_bound) for every existing
    partition of `parent`. The DEFAULT partition appears with
    (name, None, None)."""
    rows: Iterable = session.execute(
        text(
            """
        SELECT c.relname AS child_name,
               pg_get_expr(c.relpartbound, c.oid) AS bound_expr
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE p.relname = :parent
        """
        ),
        {"parent": parent},
    ).fetchall()

    out: List[Tuple[str, dt.datetime, dt.datetime]] = []
    for name, expr in rows:
        bounds = _parse_bound_expr(expr)
        if bounds is None:
            out.append((name, None, None))  # type: ignore[arg-type]
        else:
            lower, upper = bounds
            out.append((name, lower, upper))
    return out


def _parse_bound_expr(expr: str):
    """Parse pg_get_expr output for a RANGE partition.

    Examples:
        FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00')
        DEFAULT
    """
    if expr is None or "DEFAULT" in expr:
        return None
    # The expression is well-formed Postgres output; parse the two
    # quoted timestamps in order.
    parts = expr.split("'")
    if len(parts) < 5:
        return None
    try:
        lower = dt.datetime.fromisoformat(parts[1])
        upper = dt.datetime.fromisoformat(parts[3])
    except ValueError:
        return None
    if lower.tzinfo is None:
        lower = lower.replace(tzinfo=dt.timezone.utc)
    if upper.tzinfo is None:
        upper = upper.replace(tzinfo=dt.timezone.utc)
    return lower, upper
