"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""
import asyncio
from logging import getLogger
from typing import (
    Dict,
    List,
    AsyncIterator,
    Sequence,
)

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

import aleph.toolkit.json as aleph_json
from aleph.chains.chain_service import ChainService
from aleph.db.accessors.pending_messages import get_next_pending_message
from aleph.db.connection import make_engine, make_session_factory
from aleph.handlers.message_handler import MessageHandler
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.p2p import singleton
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from .job_utils import (
    prepare_loop,
    MessageJob,
    MessageProcessingResult,
    ProcessedMessage,
)

LOGGER = getLogger(__name__)


class PendingMessageProcessor(MessageJob):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        mq_conn: aio_pika.abc.AbstractConnection,
        mq_message_exchange: aio_pika.abc.AbstractExchange,
    ):
        super().__init__(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
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
        return cls(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            mq_conn=mq_conn,
            mq_message_exchange=mq_message_exchange,
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
                    message = await self.message_handler.process(
                        session=session, pending_message=pending_message
                    )
                    session.commit()
                    result: MessageProcessingResult = ProcessedMessage(message)

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
                body = {"item_hash": result.item_hash}
                mq_message = aio_pika.Message(body=aleph_json.dumps(body))
                await self.mq_message_exchange.publish(
                    mq_message,
                    routing_key=f"{result.status.value}.{result.item_hash}",
                )

            yield processing_results

    def make_pipeline(self) -> AsyncIterator[Sequence[MessageProcessingResult]]:
        message_processor = self.process_messages()
        return self.publish_to_mq(message_iterator=message_processor)


async def fetch_and_process_messages_task(config: Config, shared_stats: Dict):
    # TODO: this sleep can probably be removed
    await asyncio.sleep(4)

    engine = make_engine(config=config, application_name="aleph-process")
    session_factory = make_session_factory(engine)

    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
    )
    chain_service = ChainService(
        session_factory=session_factory, storage_service=storage_service
    )
    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=chain_service,
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
    )

    while True:
        with session_factory() as session:
            try:
                message_processing_pipeline = pending_message_processor.make_pipeline()
                async for processing_results in message_processing_pipeline:
                    for result in processing_results:
                        LOGGER.info("Successfully processed %s", result.item_hash)

            except Exception as e:
                print(e)
                LOGGER.exception("Error in pending messages job")
                session.rollback()

        LOGGER.info("Waiting 1 second(s) for new pending messages...")
        await asyncio.sleep(1)


def pending_messages_subprocess(
    config_values: Dict, shared_stats: Dict, api_servers: List
):
    """
    Background task that processes all the messages received by the node.

    :param config_values: Application configuration, as a dictionary.
    :param shared_stats: Dictionary of application metrics. This dictionary is updated by othe
                         processes and must be allocated from shared memory.
    :param api_servers: List of Core Channel Nodes with an HTTP interface found on the network.
                        This list is updated by other processes and must be allocated from
                        shared memory by the caller.
    """

    setproctitle("aleph.jobs.messages_task_loop")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/messages_task_loop.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(
        fetch_and_process_messages_task(config=config, shared_stats=shared_stats)
    )
