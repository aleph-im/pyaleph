import logging

import pytest
from aio_pika.exceptions import ChannelPreconditionFailed

from aleph.toolkit.rabbitmq import declare_durable_queue


class FakeChannel:
    """Records calls like a broker would see them; the first declare can be rejected."""

    def __init__(self, reject_first_declare: bool):
        self.reject_first_declare = reject_first_declare
        self.calls: list = []
        self.closed = False

    async def declare_queue(self, **kwargs):
        self.calls.append(("declare_queue", kwargs))
        if self.closed:
            raise RuntimeError("channel is closed")
        if self.reject_first_declare and len(self.calls) == 1:
            # RabbitMQ closes the channel when it answers PRECONDITION_FAILED.
            self.closed = True
            raise ChannelPreconditionFailed("inequivalent arg 'durable'")
        return "queue"

    async def reopen(self):
        self.calls.append(("reopen",))
        self.closed = False

    async def queue_delete(self, name):
        self.calls.append(("queue_delete", name))
        if self.closed:
            raise RuntimeError("channel is closed")


@pytest.mark.asyncio
async def test_declare_durable_queue_plain_path():
    channel = FakeChannel(reject_first_declare=False)

    queue = await declare_durable_queue(channel, "my-queue", auto_delete=False)

    assert queue == "queue"
    assert channel.calls == [
        ("declare_queue", {"name": "my-queue", "durable": True, "auto_delete": False})
    ]


@pytest.mark.asyncio
async def test_declare_durable_queue_replaces_legacy_queue(caplog):
    channel = FakeChannel(reject_first_declare=True)

    with caplog.at_level(logging.WARNING):
        queue = await declare_durable_queue(channel, "my-queue")

    declare = ("declare_queue", {"name": "my-queue", "durable": True})
    assert queue == "queue"
    assert channel.calls == [
        declare,
        ("reopen",),
        ("queue_delete", "my-queue"),
        declare,
    ]
    assert any("legacy transient queue" in r.message for r in caplog.records)
