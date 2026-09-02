"""Tests for the metrics_partition cron job.

The migration creates partitions [now-12mo, now+1mo) at test time, so a
cron run with the same retention/lookahead is a no-op in steady state.
We test the moving parts by passing custom retention/lookahead values
that force the job to either create or drop partitions.
"""

import asyncio
import datetime as dt
import logging
import threading
import time

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from sqlalchemy import text

from aleph.db.models import CrnMetricDb, MessageDb
from aleph.db.models.cron_jobs import CronJobDb
from aleph.jobs.cron.metrics_partition_job import (
    MetricsPartitionCronJob,
    _list_partitions,
    _parse_bound_expr,
)
from aleph.toolkit.partitions import add_months, month_floor, partition_name
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory


def _now_month() -> dt.datetime:
    return month_floor(dt.datetime.now(tz=dt.timezone.utc))


def _list(session, table):
    return _list_partitions(session, table)


def test_parse_bound_expr_range():
    expr = "FOR VALUES FROM ('2026-05-01 00:00:00+00') " "TO ('2026-06-01 00:00:00+00')"
    lower, upper = _parse_bound_expr(expr)
    assert lower == dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc)
    assert upper == dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)


def test_parse_bound_expr_default_returns_none():
    assert _parse_bound_expr("DEFAULT") is None


def test_parse_bound_expr_with_type_cast():
    # pg_get_expr may include the type annotation, e.g. when the column
    # type isn't unambiguous from context.
    expr = (
        "FOR VALUES FROM ('2026-05-01 00:00:00+00'::timestamp with time zone) "
        "TO ('2026-06-01 00:00:00+00'::timestamp with time zone)"
    )
    lower, upper = _parse_bound_expr(expr)
    assert lower == dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc)
    assert upper == dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)


def _make_cron_row() -> CronJobDb:
    return CronJobDb(
        id="metrics_partition",
        interval=86400,
        last_run=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
    )


@pytest.mark.asyncio
async def test_cron_is_noop_in_steady_state(session_factory: DbSessionFactory):
    """With the same retention/lookahead the migration used, a run
    against a fresh DB should not change the partition list."""
    job = MetricsPartitionCronJob(
        session_factory=session_factory,
        retention_months=12,
        lookahead_months=1,
    )
    with session_factory() as session:
        before_crn = sorted(n for n, _, _ in _list(session, "crn_metrics"))
        before_ccn = sorted(n for n, _, _ in _list(session, "ccn_metrics"))

    await job.run(now=dt.datetime.now(tz=dt.timezone.utc), job=_make_cron_row())

    with session_factory() as session:
        after_crn = sorted(n for n, _, _ in _list(session, "crn_metrics"))
        after_ccn = sorted(n for n, _, _ in _list(session, "ccn_metrics"))

    assert before_crn == after_crn
    assert before_ccn == after_ccn


@pytest.mark.asyncio
async def test_cron_creates_missing_future_partition(
    session_factory: DbSessionFactory,
):
    """Bumping the lookahead forces the cron to add a new partition for
    the month beyond what the migration already created."""
    now = dt.datetime.now(tz=dt.timezone.utc)
    expected_extra_month = add_months(month_floor(now), 2)
    expected_name = partition_name("crn_metrics", expected_extra_month)

    with session_factory() as session:
        existing = {n for n, _, _ in _list(session, "crn_metrics")}
    assert expected_name not in existing

    job = MetricsPartitionCronJob(
        session_factory=session_factory,
        retention_months=12,
        lookahead_months=2,
    )
    await job.run(now=now, job=_make_cron_row())

    with session_factory() as session:
        after = {n for n, _, _ in _list(session, "crn_metrics")}
    assert expected_name in after


@pytest.mark.asyncio
async def test_cron_drops_past_cutoff_partitions(
    session_factory: DbSessionFactory,
):
    """Tightening the retention horizon drops the now-out-of-range
    monthly partitions via DETACH + DROP."""
    now = dt.datetime.now(tz=dt.timezone.utc)
    now_month = month_floor(now)
    # With retention=1mo, only the partition starting now_month should remain.
    # Partitions older than (now_month - 1mo) should be dropped.
    new_cutoff = add_months(now_month, -1)

    with session_factory() as session:
        all_partitions = _list(session, "crn_metrics")
    to_drop = [
        name
        for name, lower, upper in all_partitions
        if upper is not None and upper <= new_cutoff
    ]
    assert to_drop, "Expected at least one partition past the new cutoff"

    job = MetricsPartitionCronJob(
        session_factory=session_factory,
        retention_months=1,
        lookahead_months=1,
    )
    await job.run(now=now, job=_make_cron_row())

    with session_factory() as session:
        remaining = {n for n, _, _ in _list(session, "crn_metrics")}

    for name in to_drop:
        assert name not in remaining


@pytest.mark.asyncio
async def test_cron_warns_when_default_has_rows(
    session_factory: DbSessionFactory, caplog
):
    """If a row lands in the DEFAULT partition, the next cron run logs
    a warning so operators notice."""
    item_hash = "msg-default-warn"
    far_future = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        session.add(
            MessageDb(
                item_hash=item_hash,
                type=MessageType.post,
                chain=Chain.ETH,
                sender="0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
                channel=Channel("aleph-scoring"),
                signature=None,
                item_type=ItemType.inline,
                item_content="{}",
                content={
                    "type": "aleph-network-metrics",
                    "address": "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
                    "content": {},
                    "time": 0.0,
                },
                time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                size=2,
            )
        )
        session.add(
            CrnMetricDb(
                item_hash=item_hash,
                node_id="node-far-future",
                measured_at=far_future,
                base_latency=0.1,
            )
        )
        session.commit()

        # Sanity: the row landed in the DEFAULT partition.
        default_count = session.execute(
            text("SELECT count(*) FROM crn_metrics_default")
        ).scalar()
        assert default_count == 1

    job = MetricsPartitionCronJob(
        session_factory=session_factory,
        retention_months=12,
        lookahead_months=1,
    )
    with caplog.at_level("WARNING"):
        await job.run(now=dt.datetime.now(tz=dt.timezone.utc), job=_make_cron_row())

    warnings = [r for r in caplog.records if "DEFAULT partition" in r.getMessage()]
    assert warnings, "Expected a warning about DEFAULT partition having rows"


@pytest.mark.asyncio
async def test_partition_routing_places_rows_in_correct_child(
    session_factory: DbSessionFactory,
):
    """Inserting a row with measured_at inside a child partition's
    range should land in that child, not the DEFAULT."""
    now = dt.datetime.now(tz=dt.timezone.utc)
    in_range_ts = month_floor(now) + dt.timedelta(days=2)
    expected_child = partition_name("crn_metrics", month_floor(now))

    item_hash = "msg-routing"
    with session_factory() as session:
        session.add(
            MessageDb(
                item_hash=item_hash,
                type=MessageType.post,
                chain=Chain.ETH,
                sender="0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
                channel=Channel("aleph-scoring"),
                signature=None,
                item_type=ItemType.inline,
                item_content="{}",
                content={
                    "type": "aleph-network-metrics",
                    "address": "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
                    "content": {},
                    "time": 0.0,
                },
                time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                size=2,
            )
        )
        session.add(
            CrnMetricDb(
                item_hash=item_hash,
                node_id="node-routed",
                measured_at=in_range_ts,
                base_latency=0.5,
            )
        )
        session.commit()

        child_count = session.execute(
            text(f"SELECT count(*) FROM {expected_child} WHERE item_hash = :h"),
            {"h": item_hash},
        ).scalar()
        default_count = session.execute(
            text("SELECT count(*) FROM crn_metrics_default WHERE item_hash = :h"),
            {"h": item_hash},
        ).scalar()

    assert child_count == 1
    assert default_count == 0


@pytest.mark.asyncio
async def test_cron_defers_table_when_parent_lock_is_contended(
    session_factory: DbSessionFactory, monkeypatch, caplog
):
    """Partition DDL must not block on the parent's ACCESS EXCLUSIVE lock: when a
    concurrent transaction holds it (as a scoring-metrics INSERT effectively does
    against the write path), the cron's bounded lock_timeout fires, the job defers
    that table and moves on, and the other (uncontended) table still succeeds."""
    import aleph.jobs.cron.metrics_partition_job as mpj

    monkeypatch.setattr(mpj, "PARTITION_LOCK_TIMEOUT", "200ms")

    now = dt.datetime.now(tz=dt.timezone.utc)
    expected_month = add_months(month_floor(now), 2)
    crn_expected = partition_name("crn_metrics", expected_month)
    ccn_expected = partition_name("ccn_metrics", expected_month)

    with session_factory() as session:
        assert crn_expected not in {n for n, _, _ in _list(session, "crn_metrics")}
        assert ccn_expected not in {n for n, _, _ in _list(session, "ccn_metrics")}

    job = MetricsPartitionCronJob(
        session_factory=session_factory,
        retention_months=12,
        lookahead_months=2,
    )

    # Hold ACCESS EXCLUSIVE on crn_metrics in a separate connection so the cron's
    # CREATE PARTITION on it must wait — and time out.
    with session_factory() as holder:
        holder.execute(text("LOCK TABLE crn_metrics IN ACCESS EXCLUSIVE MODE"))
        with caplog.at_level(logging.WARNING):
            # Must return (not hang or raise) despite the contended lock, and
            # report incomplete so the framework retries next tick.
            result = await job.run(now=now, job=_make_cron_row())

    assert result is False
    assert "Partition maintenance for crn_metrics deferred" in caplog.text

    with session_factory() as session:
        crn_after = {n for n, _, _ in _list(session, "crn_metrics")}
        ccn_after = {n for n, _, _ in _list(session, "ccn_metrics")}

    # crn_metrics was deferred (lock contended); ccn_metrics still maintained.
    assert crn_expected not in crn_after
    assert ccn_expected in ccn_after

    # With the lock released, a re-run completes (returns True) and creates the
    # previously-deferred crn_metrics partition.
    assert await job.run(now=now, job=_make_cron_row()) is True
    with session_factory() as session:
        assert crn_expected in {n for n, _, _ in _list(session, "crn_metrics")}


def _scoring_message(item_hash: str) -> MessageDb:
    return MessageDb(
        item_hash=item_hash,
        type=MessageType.post,
        chain=Chain.ETH,
        sender="0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
        channel=Channel("aleph-scoring"),
        signature=None,
        item_type=ItemType.inline,
        item_content="{}",
        content={
            "type": "aleph-network-metrics",
            "address": "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4",
            "content": {},
            "time": 0.0,
        },
        time=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        size=2,
    )


def _wait_for_lock_request(session, relation: str, mode: str, timeout: float) -> bool:
    """Poll pg_locks until some backend is waiting (not granted) for `mode`
    on `relation`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        waiting = session.execute(
            text(
                "SELECT count(*) FROM pg_locks "
                "WHERE NOT granted AND mode = :mode "
                "AND relation = CAST(:relation AS regclass)"
            ),
            {"mode": mode, "relation": relation},
        ).scalar()
        if waiting:
            return True
        time.sleep(0.05)
    return False


@pytest.mark.asyncio
async def test_partition_drop_does_not_deadlock_with_message_insert(
    session_factory: DbSessionFactory,
):
    """The metrics partitions carry a FK to `messages`, so dropping one takes
    an ACCESS EXCLUSIVE lock on `messages` (to remove the RI triggers). The
    message processor reads `messages` (ACCESS SHARE) before it inserts (ROW
    EXCLUSIVE) in the same transaction. If the cron takes a weaker lock on
    `messages` earlier in the same transaction (DETACH creates RI triggers
    under SHARE ROW EXCLUSIVE), the two lock upgrades form a cycle and
    PostgreSQL aborts one side. Both sides must succeed."""
    now = dt.datetime.now(tz=dt.timezone.utc)
    cutoff = add_months(month_floor(now), -1)
    with session_factory() as session:
        to_drop = [
            name
            for name, _, upper in _list(session, "crn_metrics")
            if upper is not None and upper <= cutoff
        ]
    assert to_drop, "Expected at least one partition past the new cutoff"

    job = MetricsPartitionCronJob(
        session_factory=session_factory,
        retention_months=1,
        lookahead_months=1,
    )

    insert_result: dict = {}

    def message_processor():
        # Same shape as MessageHandler.process(): existence check, then upsert,
        # in one transaction.
        with session_factory() as processor:
            processor.execute(
                text("SELECT 1 FROM messages WHERE item_hash = :h"),
                {"h": "msg-deadlock"},
            )
            try:
                # Dropping a partition needs ACCESS EXCLUSIVE on messages (RI
                # trigger removal). Wait until the cron is queued behind our
                # ACCESS SHARE before we upgrade to ROW EXCLUSIVE.
                with session_factory() as watcher:
                    blocked = _wait_for_lock_request(
                        watcher, "messages", "AccessExclusiveLock", timeout=10
                    )
                insert_result["cron_blocked"] = blocked
                processor.add(_scoring_message("msg-deadlock"))
                processor.commit()
                insert_result["ok"] = True
            except Exception as err:  # noqa: BLE001 - recorded for the assertion
                processor.rollback()
                insert_result["error"] = repr(err)

    thread = threading.Thread(target=message_processor)
    thread.start()
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, lambda: asyncio.run(job.run(now=now, job=_make_cron_row()))
        )
    finally:
        thread.join(timeout=30)
    assert not thread.is_alive(), "message processor thread hung"

    assert insert_result.get("cron_blocked") is True, "cron never reached DROP"
    assert insert_result.get("ok") is True, insert_result.get("error")
    assert result is True

    with session_factory() as session:
        remaining = {n for n, _, _ in _list(session, "crn_metrics")}
        inserted = session.execute(
            text("SELECT count(*) FROM messages WHERE item_hash = 'msg-deadlock'")
        ).scalar()
    assert inserted == 1
    for name in to_drop:
        assert name not in remaining
