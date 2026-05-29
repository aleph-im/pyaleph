import asyncio
from unittest.mock import MagicMock

import pytest

from aleph.web.controllers.messages import MessageBroadcaster, _WsClient


def _make_broadcaster() -> MessageBroadcaster:
    """Build a MessageBroadcaster with all external deps mocked."""
    config = MagicMock()
    config.websocket.max_message_connections.value = 100

    node_cache = MagicMock()

    async def _noop(*args, **kwargs):
        # Yield control so concurrent add() calls interleave deterministically.
        await asyncio.sleep(0)

    node_cache.incr = _noop
    node_cache.decr = _noop
    node_cache.decrby = _noop

    mq_conn = MagicMock()
    return MessageBroadcaster(mq_conn=mq_conn, config=config, node_cache=node_cache)


def _make_client() -> _WsClient:
    ws = MagicMock()
    ws.closed = False
    return _WsClient(ws, MagicMock(), exclude_content=False)


@pytest.mark.asyncio
async def test_concurrent_add_starts_single_consumer():
    """Two clients connecting concurrently must not each spawn an MQ consumer.

    Each extra consumer binds another queue to ``processed.*``, so every
    processed message would be delivered (and fanned out) once per consumer,
    making messages appear multiple times to every client.
    """
    broadcaster = _make_broadcaster()

    start_calls = 0
    release = asyncio.Event()

    async def fake_start_consumer():
        nonlocal start_calls
        start_calls += 1
        # Hold inside the consumer-start critical section, simulating the real
        # awaits (channel(), declare_queue(), consume()) that run before
        # _consumer_tag is assigned.
        await release.wait()
        broadcaster._consumer_tag = "tag"
        broadcaster._queue = MagicMock()

    async def fake_health_loop():
        return

    broadcaster._start_consumer = fake_start_consumer
    broadcaster._health_check_loop = fake_health_loop

    add_task = asyncio.gather(
        broadcaster.add(_make_client()),
        broadcaster.add(_make_client()),
    )
    # Let both add() coroutines reach the consumer-start decision point.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    release.set()
    await add_task

    assert start_calls == 1


@pytest.mark.asyncio
async def test_add_then_remove_starts_and_stops_consumer_once():
    """A normal connect/disconnect cycle starts and stops the consumer once,
    and the consumer lock does not deadlock the lifecycle."""
    broadcaster = _make_broadcaster()

    start_calls = 0
    stop_calls = 0

    async def fake_start_consumer():
        nonlocal start_calls
        start_calls += 1
        broadcaster._consumer_tag = "tag"
        broadcaster._queue = MagicMock()

    async def fake_stop_consumer():
        nonlocal stop_calls
        stop_calls += 1
        broadcaster._consumer_tag = None
        broadcaster._queue = None

    async def fake_health_loop():
        return

    broadcaster._start_consumer = fake_start_consumer
    broadcaster._stop_consumer = fake_stop_consumer
    broadcaster._health_check_loop = fake_health_loop

    client = _make_client()
    await asyncio.wait_for(broadcaster.add(client), timeout=1)
    assert start_calls == 1
    assert broadcaster._consumer_tag is not None

    await asyncio.wait_for(broadcaster.remove(client), timeout=1)
    assert stop_calls == 1
    assert broadcaster._consumer_tag is None
