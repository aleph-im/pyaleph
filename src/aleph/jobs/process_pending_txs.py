"""
Job in charge of loading messages stored on-chain and put them in the pending message queue.
"""

import asyncio
import logging
from typing import Dict, Optional, Set, TypeVar

import aio_pika
from configmanager import Config
from setproctitle import setproctitle
from sqlalchemy import delete

from aleph.chains.chain_service import ChainService
from aleph.chains.chaindata import ChainDataService
from aleph.db.connection import make_engine, make_session_factory
from aleph.db.models import Base, ChainTxDb
from aleph.db.models.pending_txs import PendingTxDb
from aleph.handlers.message_handler import MessageHandler
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.ipfs.service import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.toolkit.pending_queue import PendingQueueConsumer
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
from .job_utils import prepare_loop

LOGGER = logging.getLogger(__name__)


T = TypeVar("T", bound=Base)

class PendingTxProcessor:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        storage_service: StorageService,
        message_handler: MessageHandler,
        pending_tx_queue: PendingQueueConsumer[ChainTxDb],
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service
        self.message_handler = message_handler
        self.chain_data_service = ChainDataService(
            session_factory=session_factory, storage_service=storage_service
        )
        self.pending_tx_queue = pending_tx_queue

    async def handle_pending_tx(
        self, tx: ChainTxDb, seen_ids: Optional[Set[str]] = None
    ) -> None:
        LOGGER.info("%s Handling TX in block %s", tx.chain, tx.height)

        # If the chain data file is unavailable, we leave it to the pending tx
        # processor to log the content unavailable exception and retry later.
        messages = await self.chain_data_service.get_tx_messages(
            tx=tx, seen_ids=seen_ids
        )

        if messages:
            for i, message_dict in enumerate(messages):
                await self.message_handler.add_pending_message(
                    message_dict=message_dict,
                    reception_time=utc_now(),
                    tx_hash=tx.hash,
                    check_message=tx.protocol != ChainSyncProtocol.SMART_CONTRACT,
                )

        else:
            LOGGER.debug("TX contains no message")

        if messages is not None:
            # bogus or handled, we remove it.
            with self.session_factory() as session:
                session.execute(
                    delete(PendingTxDb).where(PendingTxDb.tx_hash == tx.hash),
                )
                session.commit()

    async def process_pending_txs(self, max_concurrent_tasks: int):
        """
        Process chain transactions in the Pending TX queue.
        """

        tasks: Set[asyncio.Task] = set()

        seen_offchain_hashes = set()
        seen_ids: Set[str] = set()
        LOGGER.info("handling TXs")
        async for pending_tx in self.pending_tx_queue:
            # TODO: remove this feature? It doesn't seem necessary.
            if pending_tx.protocol == ChainSyncProtocol.OFF_CHAIN_SYNC:
                if pending_tx.content in seen_offchain_hashes:
                    continue

            if len(tasks) == max_concurrent_tasks:
                done, tasks = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )

            if pending_tx.protocol == ChainSyncProtocol.OFF_CHAIN_SYNC:
                seen_offchain_hashes.add(pending_tx.content)

            tx_task = asyncio.create_task(
                self.handle_pending_tx(tx=pending_tx, seen_ids=seen_ids)
            )
            tasks.add(tx_task)

        # Wait for the last tasks
        if tasks:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)


async def handle_txs_task(config: Config):
    max_concurrent_tasks = config.aleph.jobs.pending_txs.max_concurrency.value
    await asyncio.sleep(4)

    engine = make_engine(config=config, application_name="aleph-txs")
    session_factory = make_session_factory(engine)

    node_cache = NodeCache(
        redis_host=config.redis.host.value, redis_port=config.redis.port.value
    )
    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
        node_cache=node_cache,
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
    mq_conn = await aio_pika.connect_robust(
        host=config.p2p.mq_host.value,
        port=config.rabbitmq.port.value,
        login=config.rabbitmq.username.value,
        password=config.rabbitmq.password.value,
    )
    mq_channel = await mq_conn.channel()
    mq_pending_tx_exchange = await mq_channel.declare_exchange(
        name=config.rabbitmq.pending_tx_exchange.value,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    mq_pending_tx_queue = await mq_channel.declare_queue(
        auto_delete=True, exclusive=True
    )
    await mq_pending_tx_queue.bind(mq_pending_tx_exchange)

    with session_factory() as session:
        pending_tx_queue = PendingQueueConsumer(
            mq_queue=mq_pending_tx_queue,
            session=session,
            db_model=ChainTxDb,
            id_key="hash",
        )

        pending_tx_processor = PendingTxProcessor(
            session_factory=session_factory,
            storage_service=storage_service,
            message_handler=message_handler,
            pending_tx_queue=pending_tx_queue,
        )

        while True:
            try:
                await pending_tx_processor.process_pending_txs(max_concurrent_tasks)
                await asyncio.sleep(5)
            except Exception:
                LOGGER.exception("Error in pending txs job")

            await asyncio.sleep(0.01)


def pending_txs_subprocess(config_values: Dict):
    setproctitle("aleph.jobs.txs_task_loop")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/txs_task_loop.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )

    loop.run_until_complete(handle_txs_task(config))
