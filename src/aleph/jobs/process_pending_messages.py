"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
import time
from logging import getLogger
from typing import Dict, List, Set

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

import aleph.toolkit.json as aleph_json
from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.pending_messages import (
    get_next_pending_messages_from_different_senders,
    get_sender_with_pending_batch,
)
from aleph.db.connection import make_async_engine, make_async_session_factory
from aleph.handlers.message_handler import MessageHandler
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import AsyncDbSessionFactory

from ..db.models import PendingMessageDb
from ..schemas.message_processing import (
    BatchMessagePayload,
    BatchResultPayload,
    SingleMessagePayload,
    SingleResultPayload,
    parse_result_payload,
)
from ..types.message_status import MessageOrigin
from .job_utils import MessageJob, prepare_loop

LOGGER = getLogger(__name__)


class PendingMessageProcessor(MessageJob):
    """
    Process pending messages by distributing them to workers via RabbitMQ.

    This class is responsible for:
    1. Finding eligible pending messages from different senders
    2. Queueing them for processing by worker processes
    3. Receiving and handling results from workers
    """

    def __init__(
        self,
        session_factory: AsyncDbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        mq_conn: aio_pika.abc.AbstractConnection,
        mq_message_exchange: aio_pika.abc.AbstractExchange,
        pending_message_queue: aio_pika.abc.AbstractQueue,
        processing_exchange: aio_pika.abc.AbstractExchange,
        result_queue: aio_pika.abc.AbstractQueue,
        worker_count: int,
        message_count: int,
    ):
        super().__init__(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            pending_message_queue=pending_message_queue,
        )

        self.mq_conn = mq_conn
        self.mq_message_exchange = mq_message_exchange
        self.processing_exchange = processing_exchange
        self.result_queue = result_queue
        self.in_progress_senders: Set[str] = set()
        self.in_progress_hashes: Set[str] = set()
        self.worker_count = worker_count
        self.message_count = message_count
        self.batch_processing_count = 0

        # Monitoring of processing
        self.processed_count = 0
        self.last_count_time = time.monotonic()
        self.messages_per_second = 0.0
        self._stats_task = None

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
        message_exchange_name: str,
        pending_message_exchange_name: str,
        processing_exchange_name: str,
        result_exchange_name: str,
        worker_count: int,
        message_count: int,
    ):
        result_queue_name = "aleph.processing.results"

        mq_conn = await aio_pika.connect_robust(
            host=mq_host, port=mq_port, login=mq_username, password=mq_password
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

        processing_exchange = await channel.declare_exchange(
            name=processing_exchange_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=False,
            auto_delete=False,
        )

        result_exchange = await channel.declare_exchange(
            name=result_exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
            auto_delete=False,
        )

        result_queue = await channel.declare_queue(
            name=result_queue_name, durable=True, auto_delete=False
        )

        await result_queue.bind(result_exchange, routing_key="#")

        return cls(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            mq_conn=mq_conn,
            mq_message_exchange=mq_message_exchange,
            pending_message_queue=pending_message_queue,
            processing_exchange=processing_exchange,
            result_queue=result_queue,
            worker_count=worker_count,
            message_count=message_count,
        )

    async def close(self):
        if self._stats_task and not self._stats_task.done():
            self._stats_task.cancel()
        await self.mq_conn.close()

    async def setup_result_consumer(self):
        """
        Set up a consumer to receive results from workers.
        """
        await self.result_queue.consume(self.handle_worker_result, no_ack=False)

    async def handle_worker_result(self, message: aio_pika.abc.AbstractIncomingMessage):
        """
        Handle a result message from a worker.

        This is called by aio_pika when a message is received on the result queue.
        Creates a task to handle the result asynchronously to avoid blocking.
        """
        await self._process_worker_result(message)

    async def _process_worker_result(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ):
        """
        Process a worker result message asynchronously.
        """
        try:
            payload = parse_result_payload(message.body.decode())

            result = payload.result
            sender = payload.sender

            LOGGER.info(
                f"Processing result for message {result.item_hash} from {sender} with status {result.status.value}"
            )

            try:
                was_in_progress = result.item_hash in self.in_progress_hashes
                self.in_progress_hashes.discard(result.item_hash)

                should_remove_sender = isinstance(payload, SingleResultPayload) or (
                    isinstance(payload, BatchResultPayload) and payload.is_last
                )

                if sender and should_remove_sender:
                    was_sender_in_progress = sender in self.in_progress_senders
                    self.in_progress_senders.discard(sender)
                    LOGGER.debug(
                        f"Removed sender {sender} from in-progress list (was: {was_sender_in_progress})"
                    )

                    # If this was the last message in a batch, release the worker
                    if isinstance(payload, BatchResultPayload) and payload.is_last:
                        self.batch_processing_count = max(
                            0, self.batch_processing_count - 1
                        )
                        LOGGER.info(
                            f"Released batch worker after completing batch from {sender}, count: {self.batch_processing_count}"
                        )

                LOGGER.debug(
                    f"Removed item_hash {result.item_hash} (was: {was_in_progress})"
                )

                # Some Monitoring for processing rate (not that usefull)
                self.processed_count += 1
                now = time.monotonic()
                elapsed = now - self.last_count_time

                if elapsed >= 5.0:
                    self.messages_per_second = self.processed_count / elapsed
                    LOGGER.info(
                        f"Processing rate: {self.messages_per_second:.2f} msg/s"
                    )
                    self.processed_count = 0
                    self.last_count_time = now

            except Exception as cleanup_error:
                LOGGER.error(f"Cleanup error: {cleanup_error}")

            await message.ack()

            try:
                if result.origin != MessageOrigin.ONCHAIN:
                    mq_message = aio_pika.Message(
                        body=aleph_json.dumps(result.to_dict()),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    )
                    await self.mq_message_exchange.publish(
                        mq_message,
                        routing_key=f"{result.status.value}.{result.item_hash}",
                    )
            except Exception as repub_error:
                LOGGER.error(f"Error republishing result: {repub_error}")

        except Exception as e:
            LOGGER.error(f"Unhandled error in handle_worker_result: {e}", exc_info=True)
            try:
                await message.nack(requeue=True)
            except Exception as nack_error:
                LOGGER.error(f"Failed to nack message: {nack_error}")

    async def _dispatch_message(self, pending_message: PendingMessageDb):
        item_hash = pending_message.item_hash
        sender = pending_message.sender

        self.in_progress_hashes.add(item_hash)
        self.in_progress_senders.add(sender)

        try:
            payload = SingleMessagePayload(
                type="single",
                message_id=pending_message.id,
                item_hash=item_hash,
                sender=sender,
            )

            mq_message = aio_pika.Message(
                body=payload.model_dump_json().encode(),
                message_id=item_hash,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await self.processing_exchange.publish(
                mq_message,
                routing_key="pending",
            )
            LOGGER.debug(
                f"Queued message {item_hash} from {sender} for processing by worker"
            )
        except Exception as e:
            LOGGER.error(f"Failed to queue message {item_hash}: {e}")
            self.in_progress_hashes.discard(item_hash)

    async def _fetch_pending_messages(self, message_limit):
        async with self.session_factory() as session:
            messages = await get_next_pending_messages_from_different_senders(
                session=session,
                current_time=utc_now(),
                fetched=True,
                exclude_item_hashes=self.in_progress_hashes,
                exclude_addresses=self.in_progress_senders,
                limit=message_limit,
            )
            if messages:
                # Here we ensure that worker will have enough message to process
                message_count = len(messages)
                if message_count == message_limit:
                    await asyncio.gather(
                        *(self._dispatch_message(msg) for msg in messages)
                    )
                    return

                LOGGER.debug(
                    f"Not enough message {message_count}/{message_limit}, launching batch processing"
                )

                # Here we will be underprocessing to avoid that we will make worker process batch for a specific sender
                sender, batch_messages = await get_sender_with_pending_batch(
                    session=session,
                    current_time=utc_now(),
                    exclude_addresses=self.in_progress_senders,
                    exclude_item_hashes=self.in_progress_hashes,
                    candidate_senders={msg.content_address for msg in messages},
                    batch_size=100,  # We try to get max 100 message
                )

                if batch_messages:
                    if self.batch_processing_count < self.worker_count - 1:
                        LOGGER.debug(
                            f"Reserving a worker for batch processing of {len(batch_messages)} messages from {sender}"
                        )
                        await self._dispatch_message_batch(batch_messages)
                    else:
                        LOGGER.debug(
                            f"Skipping batch processing as maximum batch workers ({self.worker_count-1}) are already allocated"
                        )
                        await asyncio.gather(
                            *(
                                self._dispatch_message(msg)
                                for msg in batch_messages[
                                    : self.worker_count - self.batch_processing_count
                                ]
                            )
                        )

                    # Process remaining messages from different address
                    remaining = [
                        msg for msg in messages if msg.content_address != sender
                    ]
                    await asyncio.gather(
                        *(self._dispatch_message(msg) for msg in remaining)
                    )
                else:
                    # If no batch found, just process the individual messages we found
                    await asyncio.gather(
                        *(self._dispatch_message(msg) for msg in messages)
                    )

            else:
                LOGGER.debug("No pending messages found to process")

    async def _dispatch_message_batch(self, messages: List[PendingMessageDb]) -> None:
        """
        Dispatch a batch of messages from the same sender as a single MQ message.

        This minimizes Rabbit overhead and ensures in-progress tracking is respected.
        """
        if not messages:
            return

        sender = messages[0].sender

        if not all(msg.sender == sender for msg in messages):
            LOGGER.error("Attempted to dispatch batch with mixed senders")
            return

        self.in_progress_senders.add(sender)
        self.batch_processing_count += 1
        LOGGER.info(
            f"Reserved worker for batch processing, current count: {self.batch_processing_count}"
        )

        item_hashes = []
        message_ids = []

        for msg in messages:
            self.in_progress_hashes.add(msg.item_hash)
            item_hashes.append(msg.item_hash)
            message_ids.append(msg.id)

        try:
            payload = BatchMessagePayload(
                type="batch",
                sender=sender,
                item_hashes=item_hashes,
                message_ids=message_ids,
            )

            mq_message = aio_pika.Message(
                body=payload.model_dump_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )

            await self.processing_exchange.publish(
                mq_message,
                routing_key="pending",
            )

            LOGGER.info(f"Queued batch of {len(messages)} messages from {sender}")
        except Exception as e:
            LOGGER.error(f"Failed to dispatch batch from {sender}: {e}")
            # Roll back in-progress tracking
            for h in item_hashes:
                self.in_progress_hashes.discard(h)
            self.in_progress_senders.discard(sender)
            # Release the worker reservation
            self.batch_processing_count = max(0, self.batch_processing_count - 1)
            LOGGER.info(
                f"Released batch processing worker reservation, current count: {self.batch_processing_count}"
            )

    async def process_messages(
        self,
    ) -> None:
        """
        Continuously fetch and queue pending messages if there's capacity,
        and yield results when workers return them.
        """
        self.batch_processing_count = 0

        while True:
            try:
                # Calculate effective worker count (accounting for batch processing)
                # Always keep at least 1 worker available for single messages
                effective_worker_count = max(
                    1, self.worker_count - self.batch_processing_count
                )

                # Worker capacity is based on individual messages per worker (40) for regular processing
                # 40 might need change might be "capped" form the cpu of the server used for making this feature
                worker_capacity = (effective_worker_count * 40) - len(
                    self.in_progress_senders
                )

                # Usefull debug
                LOGGER.debug(
                    f"Current worker capacity: {worker_capacity}, "
                    f"in-progress senders: {len(self.in_progress_senders)}, "
                    f"batch_processing: {self.batch_processing_count}, "
                    f"effective workers: {effective_worker_count}/{self.worker_count}"
                )

                if worker_capacity > 0:
                    await self._fetch_pending_messages(worker_capacity)

                await asyncio.sleep(0.001)
            except Exception as e:
                LOGGER.error(f"Error in process_messages: {e}")
                await asyncio.sleep(1)

    async def make_pipeline(self):
        """
        Run message processing loop.
        Note: The result consumer callback operates independently via the consumer
        we set up earlier, so we don't need to start a task for it here.
        """
        LOGGER.info(
            "Starting message processing with initial rate: 0.00 messages/second"
        )

        await self.process_messages()


async def fetch_and_process_messages_task(config: Config):
    """
    Main task function that sets up and runs the message processing pipeline.

    This function:
    1. Sets up all necessary services and connections
    2. Creates the message processor
    3. Starts the consumer to receive results from workers
    4. Runs the message processing pipeline to send messages to workers
    """
    LOGGER.info("Starting fetch_and_process_messages_task")
    engine = make_async_engine(config=config, application_name="aleph-process")
    session_factory = make_async_session_factory(engine)

    async with (
        NodeCache(
            redis_host=config.redis.host.value, redis_port=config.redis.port.value
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
            result_exchange_name=config.rabbitmq.message_result_exchange.value,
            processing_exchange_name=config.rabbitmq.message_processing_exchange.value,
            worker_count=config.aleph.jobs.message_workers.count,
            message_count=config.aleph.jobs.message_workers.message_count,
        )

        async with pending_message_processor:
            try:
                LOGGER.info("Setting up result consumer")
                await pending_message_processor.setup_result_consumer()
                LOGGER.info("Starting message processing pipeline")
                await pending_message_processor.make_pipeline()
            except asyncio.CancelledError:
                LOGGER.info("Task cancelled, shutting down")
            except Exception as e:
                LOGGER.exception(f"Unhandled error in message processing task: {e}")

            LOGGER.info("Message processing task completed")


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
