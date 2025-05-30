"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from logging import getLogger
from typing import AsyncIterator, Dict, List, Sequence, Set

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

import aleph.toolkit.json as aleph_json
from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.pending_messages import (
    async_get_next_pending_messages_from_different_senders,
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
from aleph.types.message_processing_result import MessageProcessingResult

from ..db.models import PendingMessageDb
from ..types.message_status import MessageOrigin
from .job_utils import MessageJob, prepare_loop

LOGGER = getLogger(__name__)


class PendingMessageProcessor(MessageJob):
    def __init__(
        self,
        session_factory: AsyncDbSessionFactory,
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

        # Reduced from 100 to 30 to prevent overwhelming the event loop
        self.max_parallel = 25
        self.processed_hashes: Set[str] = set()
        self.current_address: Set[str] = set()
        self._task: Dict[str, asyncio.Task] = {}

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
    ):
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

    async def process_message(
        self, pending_message: PendingMessageDb
    ) -> MessageProcessingResult:
        async with self.session_factory() as session:
            try:
                LOGGER.info(f"Processing {pending_message.item_hash}: {pending_message.content['address']}")
                result: MessageProcessingResult = await self.message_handler.process(
                    session=session, pending_message=pending_message
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                result = await self.handle_processing_error(
                    session=session,
                    pending_message=pending_message,
                    exception=e,
                )
                await session.commit()

            # Clean up tracking after processing is complete
            address = pending_message.content.get("address")
            if address in self.current_address:
                self.current_address.remove(address)

            if pending_message.item_hash in self.processed_hashes:
                self.processed_hashes.remove(pending_message.item_hash)


            return result

    async def process_messages(
        self,
    ) -> AsyncIterator[Sequence[MessageProcessingResult]]:
        """
        Process pending messages in parallel, up to max_parallel tasks at once.
        
        This method has been improved to:
        1. Better handle task completion and cleanup
        2. Process completed results more efficiently
        3. Ensure the event loop doesn't get blocked
        4. Add more robust error handling
        
        Returns:
            An async iterator yielding sequences of message processing results
        """
        # Keep track of active tasks and results
        completed_results: List[MessageProcessingResult] = []
        
        # Track the last time we yielded results
        last_yield_time = asyncio.get_event_loop().time()

        # Define a callback function when a task completes
        def task_done_callback(task: asyncio.Task):
            try:
                # The callback just ensures exceptions are logged
                # Actual result processing is done in the main loop
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                LOGGER.exception(f"Task failed with exception: {e}")

        while True:
            # Yield control back to the event loop to prevent blocking
            await asyncio.sleep(0.1)
            
            current_time = asyncio.get_event_loop().time()

            # First clean up completed tasks to free up slots
            completed_count = 0
            for item_hash, task in list(self._task.items()):
                if task.done():
                    try:
                        result = task.result()
                        if result is not None:
                            LOGGER.info(f"Message {item_hash} processed with status: {result.status}")
                            completed_results.append(result)
                            completed_count += 1
                    except asyncio.CancelledError:
                        LOGGER.debug(f"Task for {item_hash} was cancelled")
                    except Exception as e:
                        LOGGER.exception(f"Error getting task result for {item_hash}: {e}")
                    
                    # Remove from task dictionary and tracking sets
                    self._task.pop(item_hash)
                    
                    # Remove from tracking sets (clean up happens in process_message,
                    # but we also do it here as a safeguard)
                    if item_hash in self.processed_hashes:
                        self.processed_hashes.remove(item_hash)
            
            # Only fetch new messages if we're below max_parallel
            if len(self._task) < self.max_parallel:
                available_slots = self.max_parallel - len(self._task)
                
                try:
                    async with self.session_factory() as session:
                        messages: List[PendingMessageDb] = (
                            await async_get_next_pending_messages_from_different_senders(
                                session=session,
                                current_time=utc_now(),
                                fetched=True,
                                exclude_item_hashes=self.processed_hashes,
                                exclude_addresses=self.current_address,
                                limit=available_slots,
                            )
                        )
                        
                        if messages:
                            LOGGER.info(f"Fetched: {len(messages)} messages")
                            
                            # Create tasks for new messages
                            for message in messages:
                                if (
                                    not message.content
                                    or not isinstance(message.content, dict)
                                    or "address" not in message.content
                                ):
                                    continue

                                # Track processed hashes and addresses
                                item_hash = message.item_hash
                                address = message.content.get("address")

                                # Add to tracking sets
                                self.processed_hashes.add(item_hash)
                                self.current_address.add(address)

                                LOGGER.info(f"Processing {item_hash}, {address}")
                                
                                # Create task and add callback
                                task = asyncio.create_task(self.process_message(message))
                                task.add_done_callback(task_done_callback)

                                # Store in active tasks dictionary
                                self._task[item_hash] = task
                except Exception as e:
                    LOGGER.exception(f"Error fetching pending messages: {e}")
                    # Sleep a bit longer if we hit an error
                    await asyncio.sleep(1.0)

            # Yield completed results if we have any or if enough time has passed
            if completed_results and (completed_count > 0 or current_time - last_yield_time > 5.0):
                LOGGER.info(f"Yielding {len(completed_results)} completed results")
                yield completed_results
                completed_results = []
                last_yield_time = current_time
                
            # If we have no active tasks and no results, sleep a bit longer
            # to avoid spinning the CPU when there's nothing to do
            if not self._task and not completed_results:
                await asyncio.sleep(0.5)

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
        )

        async with pending_message_processor:
            while True:
                async with session_factory() as session:
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
                        await session.rollback()

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
