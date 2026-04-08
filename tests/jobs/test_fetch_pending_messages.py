"""Tests for the PendingMessageFetcher worker pool refactor.

These tests cover the four scenarios called out in code review:
- the DB connection pool is not starved by the outer loop
- successfully fetched messages are yielded to the consumer
- in-flight tasks are drained on shutdown / consumer break
- ``busy_hashes`` prevents the same item_hash from being fetched twice
"""

import asyncio
import datetime as dt
from typing import AsyncGenerator, Dict, Sequence, cast

import pytest
import pytest_asyncio
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.pending_messages import make_pending_message_fetched_statement
from aleph.db.models import MessageDb, PendingMessageDb
from aleph.jobs.fetch_pending_messages import (
    ACTIVE_FETCH_TASKS_KEY,
    MetricState,
    PendingMessageFetcher,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory


def _make_pending(
    *,
    item_hash: str,
    sender: str = "0xsender",
    next_attempt: dt.datetime = dt.datetime(2023, 1, 1),
    fetched: bool = False,
) -> PendingMessageDb:
    return PendingMessageDb(
        item_hash=item_hash,
        type=MessageType.post.value,
        chain=Chain.ETH,
        sender=sender,
        signature=None,
        item_type=ItemType.inline,
        item_content="{}",
        time=timestamp_to_datetime(1700000000),
        channel=None,
        reception_time=timestamp_to_datetime(1700000000),
        fetched=fetched,
        check_message=False,
        retries=0,
        next_attempt=next_attempt,
    )


@pytest_asyncio.fixture
async def fetcher(session_factory: DbSessionFactory, mocker) -> PendingMessageFetcher:
    """A fetcher with all external dependencies mocked.

    The MessageHandler is replaced with an AsyncMock so the per-message worker
    can be controlled from each test. The MQ queue/exchange are AsyncMocks too.
    """
    message_handler = mocker.AsyncMock()
    pending_message_queue = mocker.MagicMock()
    pending_message_exchange = mocker.AsyncMock()

    return PendingMessageFetcher(
        session_factory=session_factory,
        message_handler=message_handler,
        max_retries=3,
        pending_message_queue=pending_message_queue,
        pending_message_exchange=pending_message_exchange,
    )


# ----- helper-level unit tests -----------------------------------------------


def test_claim_messages_returns_empty_when_no_slots(
    session_factory: DbSessionFactory, fetcher: PendingMessageFetcher
):
    """``slots <= 0`` short-circuits before opening a session."""
    with session_factory() as session:
        session.add(_make_pending(item_hash="a" * 64))
        session.commit()

    assert fetcher._claim_messages(slots=0, busy_hashes=set()) == []
    assert fetcher._claim_messages(slots=-5, busy_hashes=set()) == []


def test_claim_messages_excludes_busy_hashes(
    session_factory: DbSessionFactory, fetcher: PendingMessageFetcher
):
    """Messages already in flight must not be re-claimed."""
    busy_hash = "a" * 64
    other_hash = "b" * 64
    with session_factory() as session:
        session.add(_make_pending(item_hash=busy_hash))
        session.add(_make_pending(item_hash=other_hash))
        session.commit()

    claimed = fetcher._claim_messages(slots=10, busy_hashes={busy_hash})
    claimed_hashes = [m.item_hash for m in claimed]

    assert busy_hash not in claimed_hashes
    assert other_hash in claimed_hashes


def test_claim_messages_respects_slot_limit(
    session_factory: DbSessionFactory, fetcher: PendingMessageFetcher
):
    """The query LIMIT honours the requested slot count."""
    with session_factory() as session:
        for i in range(5):
            session.add(_make_pending(item_hash=f"{i:064x}"))
        session.commit()

    claimed = fetcher._claim_messages(slots=3, busy_hashes=set())
    assert len(claimed) == 3


def test_spawn_skips_already_busy_hash(fetcher: PendingMessageFetcher, mocker):
    """``_spawn`` is a no-op if the message is already being fetched."""
    busy_hashes = {"a" * 64}
    in_flight: Dict[asyncio.Task, PendingMessageDb] = {}
    pending = _make_pending(item_hash="a" * 64)

    fetcher._spawn(pending, in_flight, busy_hashes)

    assert in_flight == {}


@pytest.mark.asyncio
async def test_publish_metric_only_writes_on_change(
    fetcher: PendingMessageFetcher, mocker
):
    """The metric is debounced — no Redis traffic when the value is unchanged."""
    node_cache = mocker.AsyncMock()
    state: MetricState = {"last": -1}

    await fetcher._publish_metric(node_cache, current=5, state=state)
    await fetcher._publish_metric(node_cache, current=5, state=state)
    await fetcher._publish_metric(node_cache, current=5, state=state)

    node_cache.set.assert_called_once_with(ACTIVE_FETCH_TASKS_KEY, 5)
    assert state["last"] == 5

    await fetcher._publish_metric(node_cache, current=6, state=state)
    assert node_cache.set.call_count == 2
    assert state["last"] == 6


@pytest.mark.asyncio
async def test_drain_cancels_and_clears_in_flight(fetcher: PendingMessageFetcher):
    """``_drain`` cancels every in-flight task and empties the dict."""

    async def _hang():
        await asyncio.sleep(60)

    in_flight: Dict[asyncio.Task, PendingMessageDb] = {}
    for i in range(3):
        task = asyncio.create_task(_hang())
        in_flight[task] = _make_pending(item_hash=f"{i:064x}")

    await fetcher._drain(in_flight)

    assert in_flight == {}


# ----- main loop integration tests -------------------------------------------


@pytest.mark.asyncio
async def test_fetch_pending_messages_yields_fetched(
    fetcher: PendingMessageFetcher,
    session_factory: DbSessionFactory,
    mock_config,
    mocker,
):
    """The generator yields a non-empty MessageDb list when fetches succeed."""
    pending_hash = "a" * 64
    with session_factory() as session:
        session.add(_make_pending(item_hash=pending_hash))
        session.commit()

    async def _fake_fetch(pending_message: PendingMessageDb):
        # Mirror the real worker: mark the row as fetched so the loop doesn't
        # re-claim it forever.
        with session_factory() as session:
            session.execute(make_pending_message_fetched_statement(pending_message, {}))
            session.commit()
        message = mocker.MagicMock(spec=MessageDb)
        message.item_hash = pending_message.item_hash
        return message

    mocker.patch.object(fetcher, "fetch_pending_message", side_effect=_fake_fetch)

    node_cache = mocker.AsyncMock()
    pipeline = fetcher.fetch_pending_messages(
        config=mock_config, node_cache=node_cache, loop=False
    )

    yielded: list[MessageDb] = []
    async for batch in pipeline:
        yielded.extend(batch)

    assert pending_hash in {m.item_hash for m in yielded}


@pytest.mark.asyncio
async def test_fetch_pending_messages_does_not_starve_pool(
    fetcher: PendingMessageFetcher,
    session_factory: DbSessionFactory,
    mock_config,
    mocker,
):
    """The outer loop must not hold a session across an await.

    Regression test for the pool starvation bug. We assert that the worker
    callable runs for many concurrent messages without the loop deadlocking
    on a session that the workers also need.
    """
    seen: list[str] = []
    expected = 5

    async def _record(pending_message: PendingMessageDb):
        # Each worker opens its own session via the real session_factory.
        # If the loop were holding the pool, this call would block.
        with session_factory() as session:
            # Mirror the real worker: mark the row as fetched so the loop
            # doesn't re-claim it forever.
            session.execute(make_pending_message_fetched_statement(pending_message, {}))
            session.commit()
            seen.append(pending_message.item_hash)
        message = mocker.MagicMock(spec=MessageDb)
        message.item_hash = pending_message.item_hash
        return message

    mocker.patch.object(fetcher, "fetch_pending_message", side_effect=_record)

    with session_factory() as session:
        for i in range(expected):
            session.add(_make_pending(item_hash=f"{i:064x}"))
        session.commit()

    node_cache = mocker.AsyncMock()
    pipeline = fetcher.fetch_pending_messages(
        config=mock_config, node_cache=node_cache, loop=False
    )

    yielded: list = []
    async for batch in pipeline:
        yielded.extend(batch)

    assert len(seen) == expected
    assert {m.item_hash for m in yielded} == set(seen)


@pytest.mark.asyncio
async def test_fetch_pending_messages_drains_on_shutdown(
    fetcher: PendingMessageFetcher,
    session_factory: DbSessionFactory,
    mock_config,
    mocker,
):
    """Closing the generator mid-flight cancels in-flight tasks and resets the metric."""
    started = asyncio.Event()
    cancelled: list[bool] = []

    async def _hang(pending_message: PendingMessageDb):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        return mocker.MagicMock(spec=MessageDb)

    mocker.patch.object(fetcher, "fetch_pending_message", side_effect=_hang)

    with session_factory() as session:
        for i in range(3):
            session.add(_make_pending(item_hash=f"{i:064x}"))
        session.commit()

    node_cache = mocker.AsyncMock()
    # fetch_pending_messages is declared as AsyncIterator but is actually an
    # async generator (uses `yield`), so we cast to expose `aclose()`.
    pipeline = cast(
        AsyncGenerator[Sequence[MessageDb], None],
        fetcher.fetch_pending_messages(
            config=mock_config, node_cache=node_cache, loop=True
        ),
    )

    consumer: asyncio.Task[Sequence[MessageDb]] = asyncio.create_task(
        pipeline.__anext__()
    )
    await started.wait()  # at least one worker is running
    await pipeline.aclose()  # triggers the generator's finally block

    consumer.cancel()
    try:
        await consumer
    except (asyncio.CancelledError, StopAsyncIteration):
        pass

    assert cancelled, "in-flight worker should have been cancelled"
    # After drain, metric is reset to 0.
    node_cache.set.assert_any_call(ACTIVE_FETCH_TASKS_KEY, 0)


@pytest.mark.asyncio
async def test_fetch_pending_messages_dedupes_concurrent_hashes(
    fetcher: PendingMessageFetcher,
    session_factory: DbSessionFactory,
    mock_config,
    mocker,
):
    """The same item_hash must not be fetched twice concurrently.

    Regression test for ``busy_hashes``. We seed a single message and assert
    that ``fetch_pending_message`` is invoked exactly once for it, even if
    the loop iterates multiple times before completion.
    """
    pending_hash = "c" * 64
    with session_factory() as session:
        session.add(_make_pending(item_hash=pending_hash))
        session.commit()

    call_count = 0

    async def _slow_fetch(pending_message: PendingMessageDb):
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        with session_factory() as session:
            session.execute(make_pending_message_fetched_statement(pending_message, {}))
            session.commit()
        message = mocker.MagicMock(spec=MessageDb)
        message.item_hash = pending_message.item_hash
        return message

    mocker.patch.object(fetcher, "fetch_pending_message", side_effect=_slow_fetch)

    node_cache = mocker.AsyncMock()
    pipeline = fetcher.fetch_pending_messages(
        config=mock_config, node_cache=node_cache, loop=False
    )

    async for _ in pipeline:
        pass

    assert call_count == 1
