import logging
from typing import List, Tuple

import pytest
from aio_pika.exceptions import ChannelPreconditionFailed

from aleph.toolkit.rabbitmq import declare_durable_queue


class FakeChannel:
    """Records calls; behaves like a channel the broker closes on a rejected declare."""

    def __init__(self, calls: List[Tuple], channel_id: int, rejections: List[bool]):
        self.calls = calls
        self.id = channel_id
        self.rejections = rejections
        self.closed = False
        self.fail_delete = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        self.calls.append(("close", self.id))

    async def declare_queue(self, **kwargs):
        self.calls.append(("declare_queue", self.id, kwargs))
        assert not self.closed
        if self.rejections and self.rejections.pop(0):
            raise ChannelPreconditionFailed("inequivalent arg 'durable'")
        return "queue"

    async def queue_delete(self, name):
        self.calls.append(("queue_delete", self.id, name))
        if self.fail_delete:
            raise RuntimeError("delete failed")


class FakeConnection:
    """
    `rejections` holds one bool per durable declare, in order: True means the broker
    answers PRECONDITION_FAILED.
    """

    def __init__(self, rejections: List[bool], fail_delete: bool = False):
        self.calls: List[Tuple] = []
        self.rejections = rejections
        self.fail_delete = fail_delete
        self.channels: List[FakeChannel] = []

    def channel(self):
        channel = FakeChannel(self.calls, len(self.channels) + 1, self.rejections)
        channel.fail_delete = self.fail_delete
        self.channels.append(channel)
        return channel


def _declares(calls):
    return [(c[0], c[1]) for c in calls if c[0] != "close"]


@pytest.mark.asyncio
async def test_declare_durable_queue_plain_path(caplog):
    connection = FakeConnection(rejections=[False])
    channel = connection.channel()  # the job's own channel, id 1

    with caplog.at_level(logging.WARNING):
        queue = await declare_durable_queue(
            connection, channel, "my-queue", auto_delete=False
        )

    kwargs = {"name": "my-queue", "durable": True, "auto_delete": False}
    assert queue == "queue"
    assert _declares(connection.calls) == [
        ("declare_queue", 2),  # probe on a temp channel
        ("declare_queue", 1),  # real declaration on the job's channel
    ]
    assert connection.calls[0] == ("declare_queue", 2, kwargs)
    assert not caplog.records


@pytest.mark.asyncio
async def test_declare_durable_queue_replaces_legacy_queue(caplog):
    connection = FakeConnection(rejections=[True, True, False])
    channel = connection.channel()

    with caplog.at_level(logging.WARNING):
        queue = await declare_durable_queue(connection, channel, "my-queue")

    assert queue == "queue"
    # probe (406), second probe on a fresh channel (406), delete on a third temp
    # channel, then the declaration on the job's channel, which never saw the 406.
    assert _declares(connection.calls) == [
        ("declare_queue", 2),
        ("declare_queue", 3),
        ("queue_delete", 4),
        ("declare_queue", 1),
    ]
    assert connection.calls[-1][1] == 1 and not channel.closed
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1
    assert "legacy transient queue" in caplog.records[0].getMessage()


@pytest.mark.asyncio
async def test_declare_durable_queue_does_not_delete_queue_replaced_by_another_job():
    # First probe fails, then another job has replaced the queue: the retry succeeds.
    connection = FakeConnection(rejections=[True, False, False])
    channel = connection.channel()

    await declare_durable_queue(connection, channel, "my-queue")

    assert [c for c in connection.calls if c[0] == "queue_delete"] == []


@pytest.mark.asyncio
async def test_declare_durable_queue_delete_failure_propagates(caplog):
    connection = FakeConnection(rejections=[True, True], fail_delete=True)
    channel = connection.channel()

    with caplog.at_level(logging.WARNING):
        with pytest.raises(RuntimeError, match="delete failed"):
            await declare_durable_queue(connection, channel, "my-queue")

    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1
