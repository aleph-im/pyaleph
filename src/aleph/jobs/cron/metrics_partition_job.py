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
short or that out-of-range data is arriving).

Locking. The metrics tables carry a foreign key to ``messages``, and every
partition inherits it, so partition DDL locks ``messages`` too: CREATE
PARTITION and DETACH create referential-integrity triggers on it (SHARE ROW
EXCLUSIVE) and DROP removes them (ACCESS EXCLUSIVE). The message processor
reads ``messages`` (ACCESS SHARE) and then upserts into it (ROW EXCLUSIVE)
within one transaction. A cron transaction that escalates its own lock on
``messages`` mid-way therefore forms a lock-upgrade cycle with any in-flight
processor transaction, and PostgreSQL aborts one of the two. To rule that
out, partition creation and each partition drop run in their own
transaction, and the drop transaction takes ACCESS EXCLUSIVE on ``messages``
as its first statement: a processor transaction that already holds a lock on
``messages`` is allowed to upgrade past the queued request and commit, after
which the cron proceeds. Every wait is bounded by ``PARTITION_LOCK_TIMEOUT``."""

import datetime as dt
import logging
from contextlib import contextmanager
from typing import Iterable, Iterator, List, Tuple

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

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

# Table the metrics partitions reference through their foreign key. See the
# module docstring for why partition DDL has to lock it.
REFERENCED_TABLE = "messages"

# Bound how long partition DDL may wait for a contended lock: the parent
# table's ACCESS EXCLUSIVE (held off by a scoring-metrics INSERT) or the
# referenced table's ACCESS EXCLUSIVE (held off by any message-processing
# transaction). Without a cap the maintenance job can queue ahead of, and
# block, the message-processing INSERT path for a long time, stalling the
# whole message-processing event loop. If a lock is contended the job defers
# this table and retries next run (partition maintenance is idempotent).
PARTITION_LOCK_TIMEOUT = "5s"


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

    async def run(self, now: dt.datetime, job: CronJobDb) -> bool:
        now_month = month_floor(now)
        cutoff = add_months(now_month, -self.retention_months)
        # Lookahead is inclusive: ensure partition for now_month + N
        # exists, so range becomes [..., now_month + N + 1).
        lookahead_upper = add_months(now_month, self.lookahead_months + 1)

        # Partition creation and each partition drop get their own transaction
        # so a lock timeout on one does not undo the others, and so no
        # transaction ever escalates a lock it already holds on the referenced
        # table (see the module docstring). Each runs under a bounded
        # lock_timeout so maintenance never blocks the message-processing
        # INSERT path.
        deferred = False
        for table in PARTITIONED_TABLES:
            try:
                with self._maintenance_transaction() as session:
                    self._ensure_partitions(session, table, now_month, lookahead_upper)
                    self._warn_if_default_has_rows(session, table)
                    to_drop = _partitions_past_cutoff(session, table, cutoff)
                for name, upper in to_drop:
                    with self._maintenance_transaction() as session:
                        self._drop_partition(session, table, name, upper, cutoff)
            except OperationalError as err:
                # A lock is contended (a scoring INSERT or a message-processing
                # transaction is in flight) and lock_timeout fired -- or another
                # transient operational error occurred. Either way, defer this
                # table rather than blocking the write path. Returning False
                # keeps last_run unadvanced so the next cron tick retries,
                # instead of waiting a full interval.
                deferred = True
                LOGGER.warning(
                    "Partition maintenance for %s deferred (%s); will retry next run",
                    table,
                    err,
                )

        return not deferred

    @contextmanager
    def _maintenance_transaction(self) -> Iterator[DbSession]:
        """One committed transaction with the bounded lock_timeout applied."""
        with self.session_factory() as session:
            session.execute(
                text(f"SET LOCAL lock_timeout = '{PARTITION_LOCK_TIMEOUT}'")
            )
            yield session
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
    def _drop_partition(
        session: DbSession,
        table: str,
        name: str,
        upper: dt.datetime,
        cutoff: dt.datetime,
    ) -> None:
        """DETACH + DROP one partition, in the caller's transaction.

        DETACH briefly takes ACCESS EXCLUSIVE on the parent, then the
        DROP only touches the now-standalone child. Metrics tables are
        not on a latency-sensitive read path so plain DETACH is fine;
        CONCURRENTLY would require autocommit, which the cron's
        transactional session doesn't offer.

        Both statements also lock the referenced table through the
        partition's foreign key (DETACH: SHARE ROW EXCLUSIVE, DROP: ACCESS
        EXCLUSIVE). Take the strongest mode first so this transaction never
        escalates a lock it already holds on it, which would deadlock with
        the message processor's own read-then-upsert escalation."""
        LOGGER.info(
            "Dropping partition %s on %s (upper=%s <= cutoff=%s)",
            name,
            table,
            upper.isoformat(),
            cutoff.isoformat(),
        )
        session.execute(text(f"LOCK TABLE {REFERENCED_TABLE} IN ACCESS EXCLUSIVE MODE"))
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


def _partitions_past_cutoff(
    session: DbSession, parent: str, cutoff: dt.datetime
) -> List[Tuple[str, dt.datetime]]:
    """Return (child_name, upper_bound) for every bounded partition of
    `parent` whose upper bound is <= cutoff. The DEFAULT partition has no
    bounds and is never returned."""
    return [
        (name, upper)
        for name, lower, upper in _list_partitions(session, parent)
        if lower is not None and upper is not None and upper <= cutoff
    ]


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
