"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from logging import getLogger
from typing import AsyncIterator, Dict, Sequence

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

import aleph.toolkit.json as aleph_json
from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.pending_messages import get_next_pending_message
from aleph.db.connection import make_engine, make_session_factory
from aleph.handlers.message_handler import MessageHandler
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_processing_result import MessageProcessingResult

from ..types.message_status import MessageOrigin
from .job_utils import MessageJob, prepare_loop

LOGGER = getLogger(__name__)


class PendingMessageProcessor(MessageJob):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        mq_conn: aio_pika.abc.AbstractConnection,
        mq_message_exchange: aio_pika.abc.AbstractExchange,
        pending_message_queue: aio_pika.abc.AbstractQueue,
    ):
        super().__init__(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            pending_message_queue=pending_message_queue,
        )

        self.mq_conn = mq_conn
        self.mq_message_exchange = mq_message_exchange

    @classmethod
    async def new(
        cls,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        mq_host: str,
        mq_port: int,
        mq_username: str,
        mq_password: str,
        message_exchange_name: str,
        pending_message_exchange_name: str,
        mq_heartbeat: int,
    ):
        mq_conn = await aio_pika.connect_robust(
            host=mq_host,
            port=mq_port,
            login=mq_username,
            password=mq_password,
            heartbeat=mq_heartbeat,
        )
        channel = await mq_conn.channel()
        mq_message_exchange = await channel.declare_exchange(
            name=message_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            auto_delete=False,
        )
        pending_message_exchange = await channel.declare_exchange(
            name=pending_message_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            auto_delete=False,
        )
        pending_message_queue = await channel.declare_queue(
            name="pending_message_queue"
        )
        await pending_message_queue.bind(
            pending_message_exchange, routing_key="process.*"
        )

        return cls(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            mq_conn=mq_conn,
            mq_message_exchange=mq_message_exchange,
            pending_message_queue=pending_message_queue,
        )

    async def close(self):
        await self.mq_conn.close()

    async def process_messages(
        self,
    ) -> AsyncIterator[Sequence[MessageProcessingResult]]:
        while True:
            with self.session_factory() as session:
                pending_message = get_next_pending_message(
                    current_time=utc_now(), session=session, fetched=True
                )
                if not pending_message:
                    break

                try:
                    result: MessageProcessingResult = (
                        await self.message_handler.process(
                            session=session, pending_message=pending_message
                        )
                    )
                    session.commit()

                except Exception as e:
                    session.rollback()
                    result = await self.handle_processing_error(
                        session=session,
                        pending_message=pending_message,
                        exception=e,
                    )
                    session.commit()

                yield [result]

    async def publish_to_mq(
        self, message_iterator: AsyncIterator[Sequence[MessageProcessingResult]]
    ) -> AsyncIterator[Sequence[MessageProcessingResult]]:
        async for processing_results in message_iterator:
            for result in processing_results:
                if result.origin != MessageOrigin.ONCHAIN:
                    mq_message = aio_pika.Message(
                        body=aleph_json.dumps(result.to_dict())
                    )
                    await self.mq_message_exchange.publish(
                        mq_message,
                        routing_key=f"{result.status.value}.{result.item_hash}",
                    )

            yield processing_results

    def make_pipeline(self) -> AsyncIterator[Sequence[MessageProcessingResult]]:
        message_processor = self.process_messages()
        return self.publish_to_mq(message_iterator=message_processor)


async def fetch_and_process_messages_task(config: Config):
    engine = make_engine(config=config, application_name="aleph-process")
    session_factory = make_session_factory(engine)

    async with (
        NodeCache(
            redis_host=config.redis.host.value,
            redis_port=config.redis.port.value,
            message_count_cache_ttl=config.perf.message_count_cache_ttl.value,
        ) as node_cache,
        IpfsService.new(config) as ipfs_service,
    ):
        storage_service = StorageService(
            storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
            ipfs_service=ipfs_service,
            node_cache=node_cache,
        )
        signature_verifier = SignatureVerifier()
        message_handler = MessageHandler(
            signature_verifier=signature_verifier,
            storage_service=storage_service,
            config=config,
        )
        pending_message_processor = await PendingMessageProcessor.new(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=config.aleph.jobs.pending_messages.max_retries.value,
            mq_host=config.p2p.mq_host.value,
            mq_port=config.rabbitmq.port.value,
            mq_username=config.rabbitmq.username.value,
            mq_password=config.rabbitmq.password.value,
            message_exchange_name=config.rabbitmq.message_exchange.value,
            pending_message_exchange_name=config.rabbitmq.pending_message_exchange.value,
            mq_heartbeat=config.rabbitmq.heartbeat.value,
        )

        async with pending_message_processor:
            while True:
                with session_factory() as session:
                    try:
                        message_processing_pipeline = (
                            pending_message_processor.make_pipeline()
                        )
                        async for processing_results in message_processing_pipeline:
                            for result in processing_results:
                                LOGGER.info(
                                    "Successfully processed %s", result.item_hash
                                )

                    except Exception:
                        LOGGER.exception("Error in pending messages job")
                        session.rollback()

                LOGGER.info("Waiting for new pending messages...")
                # We still loop periodically for retried messages as we do not bother sending a message
                # on the MQ for these.
                try:
                    await asyncio.wait_for(pending_message_processor.ready(), 1)
                except TimeoutError:
                    pass


def pending_messages_subprocess(config_values: Dict):
    """
    Background task that processes all the messages received by the node.

    :param config_values: Application configuration, as a dictionary.
    """

    setproctitle("aleph.jobs.messages_task_loop")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/messages_task_loop.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )

    loop.run_until_complete(fetch_and_process_messages_task(config=config))
