"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from logging import getLogger
from typing import (
    Dict,
    List,
    Set,
    AsyncIterator,
    Sequence,
    NewType,
)

from configmanager import Config
from setproctitle import setproctitle

from aleph.chains.chain_service import ChainService
from aleph.db.accessors.pending_messages import (
    make_pending_message_fetched_statement,
    get_next_pending_messages,
)
from aleph.db.connection import make_engine, make_session_factory
from aleph.db.models import PendingMessageDb, MessageDb
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
from .job_utils import prepare_loop, MessageJob

LOGGER = getLogger(__name__)


MessageId = NewType("MessageId", str)


class PendingMessageFetcher(MessageJob):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
    ):
        super().__init__(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
        )

    async def fetch_pending_message(self, pending_message: PendingMessageDb):
        with self.session_factory() as session:
            try:
                message = await self.message_handler.verify_and_fetch(
                    session=session, pending_message=pending_message
                )
                session.execute(
                    make_pending_message_fetched_statement(
                        pending_message, message.content
                    )
                )
                session.commit()
                return message

            except Exception as e:
                session.rollback()
                _ = await self.handle_processing_error(
                    session=session,
                    pending_message=pending_message,
                    exception=e,
                )
                session.commit()

    async def fetch_pending_messages(
        self, config: Config, shared_stats: Dict, loop: bool = True
    ) -> AsyncIterator[Sequence[MessageDb]]:
        LOGGER.info("starting fetch job")

        # Reset stats to avoid nonsensical values if the job restarts
        shared_stats["retry_messages_job_tasks"] = 0
        max_concurrent_tasks = config.aleph.jobs.pending_messages.max_concurrency.value
        fetch_tasks: Set[asyncio.Task] = set()
        task_message_dict: Dict[asyncio.Task, PendingMessageDb] = {}
        messages_being_fetched: Set[str] = set()
        fetched_messages: List[MessageDb] = []

        while True:
            with self.session_factory() as session:
                if fetch_tasks:
                    finished_tasks, fetch_tasks = await asyncio.wait(
                        fetch_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for finished_task in finished_tasks:
                        pending_message = task_message_dict.pop(finished_task)
                        messages_being_fetched.remove(pending_message.item_hash)
                        shared_stats["retry_messages_job_tasks"] -= 1

                if len(fetch_tasks) < max_concurrent_tasks:
                    pending_messages = get_next_pending_messages(
                        session=session,
                        current_time=utc_now(),
                        limit=max_concurrent_tasks - len(fetch_tasks),
                        offset=len(fetch_tasks),
                        exclude_item_hashes=messages_being_fetched,
                        fetched=False,
                    )

                    for pending_message in pending_messages:
                        # Avoid processing the same message twice at the same time.
                        if pending_message.item_hash in messages_being_fetched:
                            continue

                        # Check if the message is already processing
                        messages_being_fetched.add(pending_message.item_hash)

                        shared_stats["retry_messages_job_tasks"] += 1

                        message_task = asyncio.create_task(
                            self.fetch_pending_message(
                                pending_message=pending_message,
                            )
                        )
                        fetch_tasks.add(message_task)
                        task_message_dict[message_task] = pending_message

                if fetched_messages:
                    yield fetched_messages
                    fetched_messages = []

                if not PendingMessageDb.count(session):
                    # If not in loop mode, stop if there are no more pending messages
                    if not loop:
                        break
                    # If we are done, wait a few seconds until retrying
                    if not fetch_tasks:
                        LOGGER.info("waiting 1 second(s) for new pending messages...")
                        await asyncio.sleep(1)

    def make_pipeline(
        self,
        config: Config,
        shared_stats: Dict,
        loop: bool = True,
    ) -> AsyncIterator[Sequence[MessageDb]]:
        fetch_iterator = self.fetch_pending_messages(
            config=config, shared_stats=shared_stats, loop=loop
        )
        return fetch_iterator


async def fetch_messages_task(config: Config, shared_stats: Dict):
    # TODO: this sleep can probably be removed
    await asyncio.sleep(4)

    engine = make_engine(config=config, application_name="aleph-fetch")
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
    fetcher = PendingMessageFetcher(
        session_factory=session_factory,
        message_handler=message_handler,
        max_retries=config.aleph.jobs.pending_messages.max_retries.value,
    )

    while True:
        with session_factory() as session:
            try:
                fetch_pipeline = fetcher.make_pipeline(
                    config=config, shared_stats=shared_stats
                )
                async for fetched_messages in fetch_pipeline:
                    for fetched_message in fetched_messages:
                        LOGGER.info(
                            "Successfully fetched %s", fetched_message.item_hash
                        )

            except Exception as e:
                print(e)
                LOGGER.exception("Error in pending messages job")
                session.rollback()

        LOGGER.debug("Waiting 1 second(s) for new pending messages...")
        await asyncio.sleep(1)


def fetch_pending_messages_subprocess(
    config_values: Dict, shared_stats: Dict, api_servers: List
):
    """
    Background process that fetches all the messages received by the node.

    The goal of this process is to fetch all the data associated to an Aleph message, i.e.
    the content field of the message and any associated file. Furthermore, the process will
    validate that objects that the message depends on are already present in the database
    (ex: a message to forget, a post to amend, etc).

    :param config_values: Application configuration, as a dictionary.
    :param shared_stats: Dictionary of application metrics. This dictionary is updated by othe
                         processes and must be allocated from shared memory.
    :param api_servers: List of Core Channel Nodes with an HTTP interface found on the network.
                        This list is updated by other processes and must be allocated from
                        shared memory by the caller.
    """

    setproctitle("aleph.jobs.fetch_messages")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/fetch_messages.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    asyncio.run(fetch_messages_task(config=config, shared_stats=shared_stats))
