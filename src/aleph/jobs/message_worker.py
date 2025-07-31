"""
Standalone worker that consumes messages from RabbitMQ and processes them.
This worker can be deployed on multiple machines to scale processing horizontally.
"""

import asyncio
import time
from logging import getLogger
from typing import Dict, Optional

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.pending_messages import get_pending_message
from aleph.db.connection import make_async_engine, make_async_session_factory
from aleph.db.models.pending_messages import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.schemas.message_processing import (
    BatchMessagePayload,
    BatchResultPayload,
    ResultPayload,
    SingleMessagePayload,
    SingleResultPayload,
    parse_worker_payload,
)
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.types.db_session import AsyncDbSession, AsyncDbSessionFactory
from aleph.types.message_processing_result import MessageProcessingResult

from .job_utils import MessageJob, prepare_loop

LOGGER = getLogger(__name__)


class MessageWorker(MessageJob):
    """
    Worker that consumes messages from RabbitMQ and processes them.
    """

    def __init__(
        self,
        session_factory: AsyncDbSessionFactory,
        message_handler: MessageHandler,
        mq_conn: aio_pika.abc.AbstractConnection,
        processing_queue: aio_pika.abc.AbstractQueue,
        result_exchange: aio_pika.abc.AbstractExchange,
        worker_id: str,
        max_retries: int,
    ):
        super().__init__(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            pending_message_queue=processing_queue,
        )

        self.mq_conn = mq_conn
        self.processing_queue = processing_queue
        self.result_exchange = result_exchange
        self.worker_id = worker_id
        self.semaphore = asyncio.Semaphore(5)

    @classmethod
    async def new(
        cls,
        session_factory: AsyncDbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        mq_host: str,
        mq_port: int,
        mq_username: str,
        mq_password: str,
        processing_exchange_name: str,
        result_exchange_name: str,
        worker_id: str,
    ):
        processing_queue_name = "aleph.pending_messages"

        mq_conn = await aio_pika.connect_robust(
            host=mq_host, port=mq_port, login=mq_username, password=mq_password
        )
        channel = await mq_conn.channel()
        result_exchange = await channel.declare_exchange(
            name=result_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
            auto_delete=False,
        )

        LOGGER.info(
            f"Worker {worker_id} connected to result exchange '{result_exchange_name}'"
        )

        processing_exchange = await channel.declare_exchange(
            name=processing_exchange_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=False,
            auto_delete=False,
        )

        processing_queue = await channel.declare_queue(name=processing_queue_name)

        await processing_queue.bind(processing_exchange, routing_key="pending")

        return cls(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            mq_conn=mq_conn,
            result_exchange=result_exchange,
            processing_queue=processing_queue,
            worker_id=worker_id,
        )

    async def setup_processing_consumer(self):
        """
        Set up a consumer to receive results from workers.
        """
        await self.processing_queue.consume(
            self._process_message_callback, no_ack=False
        )

    async def _handle_single_payload(self, message, payload: SingleMessagePayload):
        LOGGER.info(
            f"Worker {self.worker_id} processing single message {payload.item_hash}"
        )

        async with self.session_factory() as session:
            start_time = time.time()

            pending_message = await get_pending_message(
                session, pending_message_id=payload.message_id
            )

            if pending_message is None:
                LOGGER.warning(
                    f"Pending message with ID {payload.message_id} for hash {payload.item_hash} not found in database"
                )
                # Acknowledge the message to avoid reprocessing
                await message.ack()
                return

            # Process the message
            result = await self._process_message(session, pending_message)
            processing_time = time.time() - start_time

            sender = payload.sender

            result_payload = SingleResultPayload(
                type="single",
                result=result,
                sender=sender,
                processing_time=processing_time,
            )

            await self._publish_result(result_payload)
            await message.ack()

            LOGGER.info(
                f"Processed single message {payload.item_hash} in {processing_time:.2f}s"
            )

    async def _handle_batch_payload(self, message, payload: BatchMessagePayload):
        LOGGER.info(
            f"Worker {self.worker_id} processing batch of {len(payload.message_ids)} messages from {payload.sender}"
        )

        # Use a separate semaphore specifically for batch items to prevent overlapping processing
        # within the same batch
        batch_semaphore = asyncio.Semaphore(1)

        async with self.session_factory() as session:
            start_batch_time = time.time()
            processed_count = 0

            for idx, msg_id in enumerate(payload.message_ids):
                async with batch_semaphore:
                    start_time = time.time()

                    pending = await get_pending_message(
                        session, pending_message_id=msg_id
                    )
                    if pending is None:
                        LOGGER.warning(
                            f"Pending message with ID {msg_id} not found in database, skipping"
                        )
                        continue

                    result = await self._process_message(session, pending)
                    processing_time = time.time() - start_time
                    processed_count += 1

                    sender = payload.sender
                    is_last = idx == len(payload.message_ids) - 1

                    result_payload = BatchResultPayload(
                        type="batch",
                        result=result,
                        sender=sender,
                        processing_time=processing_time,
                        is_last=is_last,
                    )
                    await self._publish_result(result_payload)

        batch_processing_time = time.time() - start_batch_time

        await message.ack()
        LOGGER.info(
            f"Processed {processed_count}/{len(payload.message_ids)} messages from batch for {payload.sender} in {batch_processing_time:.2f}s"
        )

    async def _process_message_callback(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        """
        Process a message from RabbitMQ.

        This is the callback that gets called when a message is received from RabbitMQ.
        It deserializes the message, processes it, and publishes the result back to RabbitMQ.
        """
        async with self.semaphore:
            try:
                raw = message.body.decode()
                payload = parse_worker_payload(raw)

                if isinstance(payload, SingleMessagePayload):
                    await self._handle_single_payload(message, payload)
                elif isinstance(payload, BatchMessagePayload):
                    await self._handle_batch_payload(message, payload)
                else:
                    LOGGER.error(f"Unknown payload type: {type(payload)}")
                    await message.ack()

                LOGGER.info(f"Worker {self.worker_id} processed message successfully")

            except ValueError as e:
                LOGGER.error(f"Error parsing message payload: {e}")
                await message.ack()
            except Exception as e:
                LOGGER.error(f"Error processing message: {e}", exc_info=True)
                await message.reject(requeue=True)

    async def _process_message(
        self,
        session: AsyncDbSession,
        pending_message: PendingMessageDb,
    ) -> MessageProcessingResult:
        """
        Process a pending message.

        This method is similar to the original process_message method in PendingMessageProcessor,
        but it actually processes the message instead of publishing it to RabbitMQ.
        """
        item_hash = pending_message.item_hash
        content = pending_message.content or {}
        sender = content.get("address", None)

        try:
            LOGGER.debug(f"Processing message {item_hash} from {sender}")
            await session.refresh(pending_message)
            result: MessageProcessingResult = await self.message_handler.process(
                session=session,
                pending_message=pending_message,
            )

            await session.commit()
            LOGGER.debug(f"Successfully processed message {item_hash} from {sender}")
        except Exception as e:
            LOGGER.warning(f"Error processing message {item_hash} from {sender}: {e}")
            await session.rollback()
            await session.refresh(pending_message, attribute_names=None)

            result = await self.handle_processing_error(
                session=session,
                pending_message=pending_message,
                exception=e,
            )

            await session.commit()

        return result

    async def _publish_result(
        self,
        payload: ResultPayload,
    ) -> None:
        """
        Publish result of processing to the result queue for PendingMessageProcessor.
        """
        if not payload:
            return

        result_payload = payload.model_dump_json().encode()

        mq_message = aio_pika.Message(
            body=result_payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        routing_key = (
            f"{payload.result.status.value}.{payload.result.item_hash}.{payload.sender}"
        )
        LOGGER.debug(f"Publishing result {routing_key}")

        await self.result_exchange.publish(
            routing_key=routing_key,
            message=mq_message,
        )

    async def run(self) -> None:
        """Run the worker."""
        await self.setup_processing_consumer()

        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            LOGGER.info(f"Worker {self.worker_id} received cancel signal")


async def run_message_worker(config: Config, worker_id: Optional[str] = None):
    """Run a message worker process."""
    if worker_id is None:
        worker_id = f"worker-{time.time_ns()}"

    LOGGER.info(f"Starting message worker {worker_id}")

    engine = make_async_engine(
        config=config, application_name=f"aleph-worker-{worker_id}"
    )
    session_factory = make_async_session_factory(engine)

    async with (
        NodeCache(
            redis_host=config.redis.host.value, redis_port=config.redis.port.value
        ) as node_cache,
        IpfsService.new(config) as ipfs_service,
    ):
        # Create storage service
        storage_service = StorageService(
            storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
            ipfs_service=ipfs_service,
            node_cache=node_cache,
        )

        # Create message handler
        signature_verifier = SignatureVerifier()
        message_handler = MessageHandler(
            signature_verifier=signature_verifier,
            storage_service=storage_service,
            config=config,
        )

        # Create worker with max_retries from config
        worker = await MessageWorker.new(
            session_factory=session_factory,
            message_handler=message_handler,
            mq_host=config.p2p.mq_host.value,
            mq_port=config.rabbitmq.port.value,
            mq_username=config.rabbitmq.username.value,
            mq_password=config.rabbitmq.password.value,
            max_retries=config.aleph.jobs.pending_messages.max_retries.value,
            processing_exchange_name=config.rabbitmq.message_processing_exchange.value,
            result_exchange_name=config.rabbitmq.message_result_exchange.value,
            worker_id=worker_id,
        )

        await worker.run()


def message_worker_subprocess(config_values: Dict, worker_id: Optional[str] = None):
    """
    Start a message worker subprocess.

    This function is called to start a new worker process.
    It sets up the process title, logging, and runs the worker.

    Args:
        config_values: Application configuration as a dictionary
        worker_id: Optional unique ID for this worker
    """
    setproctitle("aleph.jobs.message_worker")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename=f"/tmp/message_worker_{worker_id or 'default'}.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )

    loop.run_until_complete(run_message_worker(config=config, worker_id=worker_id))


if __name__ == "__main__":
    import argparse

    from aleph.config import get_config

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run an Aleph message worker")
    parser.add_argument("--worker-id", type=str, help="Unique ID for this worker")
    parser.add_argument("--config-file", type=str, help="Path to a config file")
    args = parser.parse_args()

    # Load config
    config = get_config()

    # Load config file if provided
    if args.config_file is not None:
        config.yaml.load(args.config_file)

    # Run worker
    message_worker_subprocess(config.as_dict(), args.worker_id)
