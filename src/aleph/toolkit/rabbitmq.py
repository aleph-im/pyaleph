import logging
from typing import Any

import aio_pika
import aio_pika.abc
from aio_pika.exceptions import ChannelPreconditionFailed

LOGGER = logging.getLogger(__name__)


async def make_mq_conn(config) -> aio_pika.abc.AbstractConnection:
    mq_conn = await aio_pika.connect_robust(
        host=config.p2p.mq_host.value,
        port=config.rabbitmq.port.value,
        login=config.rabbitmq.username.value,
        password=config.rabbitmq.password.value,
        heartbeat=config.rabbitmq.heartbeat.value,
    )
    return mq_conn


async def declare_durable_queue(
    channel: aio_pika.abc.AbstractChannel, name: str, **kwargs: Any
) -> aio_pika.abc.AbstractQueue:
    """
    Declares a durable queue, replacing a legacy transient queue of the same name.

    RabbitMQ 4.3 no longer accepts transient non-exclusive queues, which is what
    pyaleph used to declare. A broker that still holds such a queue answers the durable
    declaration with PRECONDITION_FAILED and closes the channel. These queues only carry
    wake-up signals (the jobs poll the database every 1 to 5 seconds), so dropping the
    old queue loses nothing.
    """
    try:
        return await channel.declare_queue(name=name, durable=True, **kwargs)
    except ChannelPreconditionFailed:
        LOGGER.warning(
            "Queue %s exists with different properties (legacy transient queue), "
            "deleting it and declaring it again as durable",
            name,
        )
        # The broker closed the channel when it rejected the declaration.
        await channel.reopen()
        await channel.queue_delete(name)
        return await channel.declare_queue(name=name, durable=True, **kwargs)
