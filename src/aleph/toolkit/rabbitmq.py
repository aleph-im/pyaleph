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
    connection: aio_pika.abc.AbstractConnection,
    channel: aio_pika.abc.AbstractChannel,
    name: str,
    **kwargs: Any,
) -> aio_pika.abc.AbstractQueue:
    """
    Declares a durable queue on `channel`, replacing a legacy transient queue of the same name.

    RabbitMQ 4.3 no longer accepts transient non-exclusive queues, which is what
    pyaleph used to declare. A broker that still holds such a queue answers the durable
    declaration with PRECONDITION_FAILED and closes the channel. The queues only carry
    wake-up signals (the jobs poll the database every 1 to 5 seconds), so dropping the
    old one loses nothing.

    The probe and the deletion use short-lived channels of `connection`, so that the
    channel of the caller never sees the 406 and does not need to be reopened.

    Several jobs declare the same queue name at start. After a first 406 the durable
    declaration is retried on a fresh channel and the queue is deleted only if that
    fails again, so a job does not delete the durable queue (with its binding and
    consumer) that another job has just created. A tiny window remains if two jobs both
    get their second 406 before either deletes; the normal upgrade path recreates the
    broker, so no legacy queue exists then.
    """
    try:
        await _probe_durable_queue(connection, name, **kwargs)
    except ChannelPreconditionFailed as e:
        LOGGER.warning(
            "Queue %s: inequivalent declaration (legacy transient queue?), "
            "replacing it with a durable queue: %s",
            name,
            e,
        )
        try:
            await _probe_durable_queue(connection, name, **kwargs)
        except ChannelPreconditionFailed:
            async with connection.channel() as delete_channel:
                await delete_channel.queue_delete(name)

    return await channel.declare_queue(name=name, durable=True, **kwargs)


async def _probe_durable_queue(
    connection: aio_pika.abc.AbstractConnection, name: str, **kwargs: Any
) -> None:
    # The broker closes the channel on PRECONDITION_FAILED, hence a throwaway channel.
    async with connection.channel() as probe_channel:
        await probe_channel.declare_queue(name=name, durable=True, **kwargs)
