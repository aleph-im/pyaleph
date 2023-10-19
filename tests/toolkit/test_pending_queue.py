import asyncio
from typing import Generator, AsyncContextManager

import aio_pika.abc
import pytest
import pytest_asyncio
from aleph_message.models import Chain
from configmanager import Config

from aleph.db.models import PendingTxDb, ChainTxDb
from aleph.toolkit.pending_queue import PendingQueuePublisher, PendingQueueConsumer
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
import datetime as dt


@pytest_asyncio.fixture()
async def mq_channel(mock_config: Config) -> AsyncContextManager[aio_pika.abc.AbstractChannel]:
    mq_conn = await aio_pika.connect_robust(
        host="localhost",
        port=mock_config.rabbitmq.port.value,
        login=mock_config.rabbitmq.username.value,
        password=mock_config.rabbitmq.password.value,
    )
    channel = await mq_conn.channel()
    try:
        yield channel
    finally:
        await channel.close()
        await mq_conn.close()


@pytest_asyncio.fixture()
async def mq_exchange(mq_channel: aio_pika.abc.AbstractChannel) -> AsyncContextManager[aio_pika.abc.AbstractExchange]:
    mq_message_exchange = await mq_channel.declare_exchange(
        name="test-pending-queue-exchange",
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    try:
        yield mq_message_exchange
    finally:
        await mq_message_exchange.delete()


@pytest_asyncio.fixture()
async def mq_queue(mq_channel: aio_pika.abc.AbstractChannel, mq_exchange: aio_pika.abc.AbstractExchange) -> AsyncContextManager[aio_pika.abc.AbstractQueue]:
    mq_queue = await mq_channel.declare_queue(name="test-pending-queue")
    await mq_queue.bind(mq_exchange, routing_key="*")
    try:
        yield mq_queue
    finally:
        await mq_queue.delete()

@pytest_asyncio.fixture
async def publisher(
    mq_exchange: aio_pika.abc.AbstractExchange,
) -> PendingQueuePublisher:
    return PendingQueuePublisher(
            mq_exchange=mq_exchange,
            db_model=PendingTxDb,
            id_key="tx_hash",
        )


async def consumer(mq_queue: aio_pika.abc.AbstractQueue) -> PendingQueueConsumer:
    return PendingQueueConsumer(mq_queue=mq_queue)


@pytest_asyncio.fixture
async def consumer(
    mq_channel: aio_pika.abc.AbstractChannel,
) -> AsyncContextManager[PendingQueueConsumer]:
    mq_message_exchange = await mq_channel.declare_exchange(
        name="test-pending-queue",
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    try:
        yield PendingQueuePublisher(
            mq_exchange=mq_message_exchange,
            db_model=PendingTxDb,
            id_key="tx_hash",
        )
    finally:
        await mq_message_exchange.delete()

@pytest.mark.asyncio
async def test_publish(
    publisher: PendingQueuePublisher,
    mq_channel: aio_pika.abc.AbstractChannel,
    session_factory: DbSessionFactory,
):
    mq_exchange = publisher.mq_exchange
    mq_queue = await mq_channel.declare_queue(name="test-pending-queue", durable=True)
    await mq_queue.bind(mq_exchange, routing_key="*")

    tx = ChainTxDb(
        hash="1234",
        chain=Chain.ETH,
        height=1000,
        datetime=dt.datetime(2023, 1, 1),
        publisher="me",
        protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
        protocol_version=1,
        content="abcdef",
    )
    pending_tx = PendingTxDb(tx=tx)

    with session_factory() as session:
        session.add(pending_tx)
        session.commit()

    try:
        await publisher.add(pending_tx)
        message = await mq_queue.get(no_ack=True, fail=False)
    finally:
        await mq_queue.delete()

    assert message
    assert message.body.decode("utf-8") == pending_tx.tx_hash
