"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
import faulthandler
import sys
from logging import getLogger
from typing import AsyncIterator, Dict, List, NewType, Sequence, Set, TypedDict

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.pending_messages import (
    get_next_pending_messages,
    make_pending_message_fetched_statement,
)
from aleph.db.connection import make_engine, make_session_factory
from aleph.db.models import MessageDb, PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory

from ..toolkit.rabbitmq import make_mq_conn
from .job_utils import MessageJob, make_pending_message_queue, prepare_loop

LOGGER = getLogger(__name__)


MessageId = NewType("MessageId", str)

# Redis key tracking the number of in-flight fetch tasks. Used by /metrics.
ACTIVE_FETCH_TASKS_KEY = "retry_messages_job_tasks"


class MetricState(TypedDict):
    """Tracks the last value sent to Redis to debounce no-op updates."""

    last: int


class PendingMessageFetcher(MessageJob):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        pending_message_queue: aio_pika.abc.AbstractQueue,
        pending_message_exchange: aio_pika.abc.AbstractExchange,
    ):
        super().__init__(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=max_retries,
            pending_message_queue=pending_message_queue,
        )
        self.pending_message_queue = pending_message_queue
        self.pending_message_exchange = pending_message_exchange

    async def _notify_message_fetched(self, pending_message: PendingMessageDb) -> None:
        """Publish to MQ to wake up the process job after a message is fetched."""
        mq_message = aio_pika.Message(body=f"{pending_message.id}".encode("utf-8"))
        await self.pending_message_exchange.publish(
            mq_message,
            routing_key=f"process.{pending_message.item_hash}",
        )

    async def fetch_pending_message(self, pending_message: PendingMessageDb):
        with self.session_factory() as session:
            try:
                message = await self.message_handler.verify_and_fetch_message(
                    pending_message=pending_message, session=session
                )

                content = message.content
                if not isinstance(content, dict):
                    raise ValueError(
                        f"Fetched message {message.item_hash} has no content dict"
                    )

                session.execute(
                    make_pending_message_fetched_statement(pending_message, content)
                )
                session.commit()

                # Notify process job that a message is ready
                await self._notify_message_fetched(pending_message)

                return message

            except Exception as e:
                session.rollback()
                _ = await self.handle_processing_error(
                    session=session,
                    pending_message=pending_message,
                    exception=e,
                )
                session.commit()
                return None

    def _claim_messages(
        self,
        slots: int,
        busy_hashes: Set[str],
    ) -> List[PendingMessageDb]:
        """Open a fresh session, fetch up to ``slots`` candidates, close immediately.

        The session is intentionally short-lived: it must NOT be held across an
        ``await`` so the connection returns to the pool while the worker tasks run.
        """
        if slots <= 0:
            return []
        with self.session_factory() as session:
            return list(
                get_next_pending_messages(
                    session=session,
                    current_time=utc_now(),
                    limit=slots,
                    exclude_item_hashes=busy_hashes,
                    fetched=False,
                )
            )

    def _spawn(
        self,
        pending_message: PendingMessageDb,
        in_flight: Dict[asyncio.Task, PendingMessageDb],
        busy_hashes: Set[str],
    ) -> None:
        """Schedule a fetch task and track it as in-flight."""
        if pending_message.item_hash in busy_hashes:
            return
        busy_hashes.add(pending_message.item_hash)
        task = asyncio.create_task(
            self.fetch_pending_message(pending_message=pending_message)
        )
        in_flight[task] = pending_message

    def _reap(
        self,
        finished: Set[asyncio.Task],
        in_flight: Dict[asyncio.Task, PendingMessageDb],
        busy_hashes: Set[str],
    ) -> List[MessageDb]:
        """Drain completed tasks; return successfully fetched ``MessageDb`` results."""
        results: List[MessageDb] = []
        for task in finished:
            pending_message = in_flight.pop(task)
            busy_hashes.discard(pending_message.item_hash)
            try:
                outcome = task.result()
            except asyncio.CancelledError:
                continue
            except Exception:
                # fetch_pending_message catches everything internally; this is
                # defensive in case a future change leaks an exception.
                # BaseException (SystemExit, KeyboardInterrupt) is intentionally
                # not caught — let it propagate to the generator's finally.
                LOGGER.exception(
                    "Unexpected exception in fetch task for %s",
                    pending_message.item_hash,
                )
                continue
            if outcome is not None:
                results.append(outcome)
        return results

    async def _drain(self, in_flight: Dict[asyncio.Task, PendingMessageDb]) -> None:
        """Cancel and await all in-flight tasks. Idempotent."""
        if not in_flight:
            return
        # task.cancel() only schedules cancellation on the next event loop tick,
        # so iterating `in_flight` here is safe — the dict is not mutated until
        # we explicitly clear it after gather.
        for task in in_flight:
            task.cancel()
        await asyncio.gather(*in_flight, return_exceptions=True)
        in_flight.clear()

    async def _publish_metric(
        self,
        node_cache: NodeCache,
        current: int,
        state: MetricState,
    ) -> None:
        """Sync the in-flight task count to Redis. No-op if unchanged.

        Replaces per-task ``incr``/``decr`` (60+ Redis roundtrips per cycle at
        ``max_concurrency=30``) with at most one ``SET`` per loop iteration.
        """
        if state["last"] == current:
            return
        await node_cache.set(ACTIVE_FETCH_TASKS_KEY, current)
        state["last"] = current

    async def fetch_pending_messages(
        self, config: Config, node_cache: NodeCache, loop: bool = True
    ) -> AsyncIterator[Sequence[MessageDb]]:
        LOGGER.info("starting fetch job")

        max_concurrent_tasks = config.aleph.jobs.pending_messages.max_concurrency.value
        idle_timeout = config.aleph.jobs.pending_messages.idle_timeout.value

        # State is local to the generator call so a make_pipeline restart after
        # an exception starts from a clean slate.
        in_flight: Dict[asyncio.Task, PendingMessageDb] = {}
        busy_hashes: Set[str] = set()
        metric_state: MetricState = {"last": -1}

        await node_cache.set(ACTIVE_FETCH_TASKS_KEY, 0)
        metric_state["last"] = 0

        try:
            while True:
                # 1. Refill the pool from the DB.
                slots = max_concurrent_tasks - len(in_flight)
                for pending_message in self._claim_messages(slots, busy_hashes):
                    self._spawn(pending_message, in_flight, busy_hashes)

                # 2. Publish updated metric (single SET, only on change).
                await self._publish_metric(node_cache, len(in_flight), metric_state)

                # 3. Idle path: nothing in flight after step 1.
                if not in_flight:
                    # In non-loop mode, "no claim and no in-flight" means there
                    # is no work *ready right now*. Future-dated retries do not
                    # block exit because _claim_messages already filters them.
                    if not loop:
                        break
                    LOGGER.info("waiting for new pending messages...")
                    try:
                        await asyncio.wait_for(self.ready(), idle_timeout)
                    except asyncio.TimeoutError:
                        pass
                    continue

                # 4. Wait for at least one task to complete.
                #    NB: no DB session is held across this await.
                finished, _ = await asyncio.wait(
                    in_flight.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                # 5. Reap all completed tasks (FIRST_COMPLETED can return >1).
                fetched = self._reap(finished, in_flight, busy_hashes)
                if fetched:
                    yield fetched
        finally:
            # Cancel and await any in-flight tasks on consumer break, exception,
            # or normal shutdown. Each task's own DB session is rolled back via
            # its `with` context manager, leaving rows as fetched=False for retry.
            await self._drain(in_flight)
            try:
                await node_cache.set(ACTIVE_FETCH_TASKS_KEY, 0)
            except Exception:
                LOGGER.warning(
                    "Failed to reset %s on drain",
                    ACTIVE_FETCH_TASKS_KEY,
                    exc_info=True,
                )

    def make_pipeline(
        self,
        config: Config,
        node_cache: NodeCache,
        loop: bool = True,
    ) -> AsyncIterator[Sequence[MessageDb]]:
        fetch_iterator = self.fetch_pending_messages(
            config=config, node_cache=node_cache, loop=loop
        )
        return fetch_iterator


async def fetch_messages_task(config: Config):
    engine = make_engine(config=config, application_name="aleph-fetch")
    session_factory = make_session_factory(engine)

    mq_conn = await make_mq_conn(config=config)
    mq_channel = await mq_conn.channel()

    pending_message_queue = await make_pending_message_queue(
        config=config, routing_key="fetch.*", channel=mq_channel
    )

    # Exchange to notify process job when messages are fetched
    pending_message_exchange = await mq_channel.declare_exchange(
        name=config.rabbitmq.pending_message_exchange.value,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )

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
        fetcher = PendingMessageFetcher(
            session_factory=session_factory,
            message_handler=message_handler,
            max_retries=config.aleph.jobs.pending_messages.max_retries.value,
            pending_message_queue=pending_message_queue,
            pending_message_exchange=pending_message_exchange,
        )

        async with fetcher:
            while True:
                try:
                    fetch_pipeline = fetcher.make_pipeline(
                        config=config, node_cache=node_cache
                    )
                    async for fetched_messages in fetch_pipeline:
                        for fetched_message in fetched_messages:
                            LOGGER.info(
                                "Successfully fetched %s", fetched_message.item_hash
                            )

                except Exception:
                    LOGGER.exception("Unexpected error in pending messages fetch job")

                LOGGER.debug("Waiting 1 second(s) for new pending messages...")
                await asyncio.sleep(1)


def fetch_pending_messages_subprocess(config_values: Dict):
    """
    Background process that fetches all the messages received by the node.

    The goal of this process is to fetch all the data associated to an Aleph message, i.e.
    the content field of the message and any associated file. Furthermore, the process will
    validate that objects that the message depends on are already present in the database
    (ex: a message to forget, a post to amend, etc).

    :param config_values: Application configuration, as a dictionary.
    """

    faulthandler.enable(file=sys.stderr)

    setproctitle("aleph.jobs.fetch_messages")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/fetch_messages.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )

    try:
        asyncio.run(fetch_messages_task(config=config))
    except KeyboardInterrupt:
        LOGGER.info("Fetch messages subprocess interrupted")
    except SystemExit:
        raise
    except BaseException:
        LOGGER.critical("Fatal error in fetch messages subprocess", exc_info=True)
        raise
