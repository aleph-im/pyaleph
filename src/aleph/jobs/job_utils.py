import asyncio
import datetime as dt
import logging
from typing import Dict, Optional, Tuple, Union

import aio_pika
from configmanager import Config
from sqlalchemy import update

import aleph.config
from aleph.db.accessors.messages import reject_existing_pending_message
from aleph.db.accessors.pending_messages import set_next_retry
from aleph.db.models import PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_processing_result import RejectedMessage, WillRetryMessage
from aleph.types.message_status import (
    ErrorCode,
    InvalidMessageException,
    MessageContentUnavailable,
    RetryMessageException,
)

LOGGER = logging.getLogger(__name__)


MAX_RETRY_INTERVAL: int = 300


async def _make_pending_queue(
    config: Config,
    exchange_name: str,
    queue_name: str,
    routing_key: str,
    channel: Optional[aio_pika.abc.AbstractChannel] = None,
) -> aio_pika.abc.AbstractQueue:
    if not channel:
        mq_conn = await aio_pika.connect_robust(
            host=config.p2p.mq_host.value,
            port=config.rabbitmq.port.value,
            login=config.rabbitmq.username.value,
            password=config.rabbitmq.password.value,
            heartbeat=config.rabbitmq.heartbeat.value,
        )
        channel = await mq_conn.channel()

    exchange = await channel.declare_exchange(
        name=exchange_name,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    queue = await channel.declare_queue(name=queue_name, auto_delete=False)
    await queue.bind(exchange, routing_key=routing_key)
    return queue


async def make_pending_tx_queue(
    config: Config, channel: aio_pika.abc.AbstractChannel
) -> aio_pika.abc.AbstractQueue:
    return await _make_pending_queue(
        config=config,
        exchange_name=config.rabbitmq.pending_tx_exchange.value,
        queue_name="pending-tx-queue",
        routing_key="#",
        channel=channel,
    )


async def make_pending_message_queue(
    config: Config,
    routing_key: str,
    channel: Optional[aio_pika.abc.AbstractChannel] = None,
) -> aio_pika.abc.AbstractQueue:
    return await _make_pending_queue(
        config=config,
        exchange_name=config.rabbitmq.pending_message_exchange.value,
        queue_name="pending_message_queue",
        routing_key=routing_key,
        channel=channel,
    )


def compute_next_retry_interval(attempts: int) -> dt.timedelta:
    """
    Computes the time interval for the next attempt/retry of a message.

    The interval is computed as 2^attempts and is capped at 5 minutes.
    The maximum amount of retries is controlled in the node configuration.

    :param attempts: Current number of attempts.
    :return: The time interval between the previous processing attempt and the next one.
    """

    seconds = 2**attempts
    return dt.timedelta(seconds=min(seconds, MAX_RETRY_INTERVAL))


def schedule_next_attempt(
    session: DbSession, pending_message: PendingMessageDb
) -> None:
    """
    Schedules the next attempt time for a failed pending message.

    :param session: DB session.
    :param pending_message: Pending message to retry.
    """

    # Set the next attempt in the future, even if the message is old. The message
    # may have failed processing because of a temporary network issue or because
    # a file could not be fetched at the moment. If we scheduled the next attempt
    # relative to the last attempt time of the message, we could enter situations
    # where the message exhausts all its retries in a few seconds.
    # If other messages depend on the rescheduled message, they will also be marked
    # rescheduled, later than the message they depend on. This guarantees that messages
    # are processed in the right order while leaving enough time for the issue that
    # caused the original message to be rescheduled to get resolved.
    next_attempt = utc_now() + compute_next_retry_interval(pending_message.retries)
    set_next_retry(
        session=session, pending_message=pending_message, next_attempt=next_attempt
    )
    pending_message.next_attempt = next_attempt
    pending_message.retries += 1


def prepare_loop(config_values: Dict) -> Tuple[asyncio.AbstractEventLoop, Config]:
    """
    Prepares all the global variables (sigh) needed to run an Aleph subprocess.

    :param config_values: Dictionary of config values, as provided by the main process.
    :returns: A preconfigured event loop, and the application config for convenience.
              Use the event loop as event loop of the process as it is used by Motor. Using another
              event loop will cause DB calls to fail.
    """

    loop = asyncio.get_event_loop()

    config = aleph.config.app_config
    config.load_values(config_values)

    return loop, config


class MqWatcher:
    """
    Watches a RabbitMQ message queue for new messages and maintains an asyncio Event object
    that tracks whether there is still work to do.

    This class is used by the tx/message processors to detect new pending objects in the database.
    We use RabbitMQ messages for signaling new pending objects but the actual objects are stored
    in the DB.

    This class is an async context manager that spawns a watcher task. Callers can use the `ready()`
    method to determine if there is work to be done.
    """

    def __init__(self, mq_queue: aio_pika.abc.AbstractQueue):
        self.mq_queue = mq_queue

        self._watcher_task = None
        self._event = asyncio.Event()

    async def _check_for_message(self):
        async with self.mq_queue.iterator(no_ack=True) as queue_iter:
            async for _ in queue_iter:
                self._event.set()

    async def __aenter__(self):
        self._watcher_task = asyncio.create_task(self._check_for_message())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._watcher_task is not None:
            self._watcher_task.cancel()
            await self._watcher_task

    async def ready(self):
        await self._event.wait()
        self._event.clear()


class MessageJob(MqWatcher):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
        pending_message_queue: aio_pika.abc.AbstractQueue,
    ):
        super().__init__(mq_queue=pending_message_queue)

        self.session_factory = session_factory
        self.message_handler = message_handler
        self.max_retries = max_retries

    @staticmethod
    def _handle_rejection(
        session: DbSession,
        pending_message: PendingMessageDb,
        exception: BaseException,
    ) -> RejectedMessage:
        rejected_message_db = reject_existing_pending_message(
            session=session,
            pending_message=pending_message,
            exception=exception,
        )
        # The call to reject the message can actually return None if the message was not
        # actually marked as rejected (ex: a valid version of the message exists).
        # In that case, determine the error code here.
        error_code = (
            rejected_message_db.error_code
            if rejected_message_db
            else getattr(exception, "error_code", ErrorCode.INTERNAL_ERROR)
        )

        return RejectedMessage(pending_message=pending_message, error_code=error_code)

    async def _handle_retry(
        self,
        session: DbSession,
        pending_message: PendingMessageDb,
        exception: BaseException,
    ) -> Union[RejectedMessage, WillRetryMessage]:
        error_code = ErrorCode.INTERNAL_ERROR
        if isinstance(exception, MessageContentUnavailable):
            LOGGER.warning(
                "Could not fetch message %s, putting it back in the fetch queue: %s",
                pending_message.item_hash,
                str(exception),
            )
            error_code = exception.error_code
            session.execute(
                update(PendingMessageDb)
                .where(PendingMessageDb.id == pending_message.id)
                .values(fetched=False)
            )
        elif not isinstance(exception, RetryMessageException):
            LOGGER.exception(
                "Unexpected error while fetching message", exc_info=exception
            )

        if pending_message.retries >= self.max_retries:
            LOGGER.warning(
                "Rejecting pending message: %s - too many retries",
                pending_message.item_hash,
            )
            return self._handle_rejection(
                session=session,
                pending_message=pending_message,
                exception=exception,
            )
        else:
            LOGGER.warning(
                "Message %s marked for retry: %s",
                pending_message.item_hash,
                str(exception),
            )
            schedule_next_attempt(session=session, pending_message=pending_message)
            return WillRetryMessage(
                pending_message=pending_message, error_code=error_code
            )

    async def handle_processing_error(
        self,
        session: DbSession,
        pending_message: PendingMessageDb,
        exception: BaseException,
    ) -> Union[RejectedMessage, WillRetryMessage]:
        if isinstance(exception, InvalidMessageException):
            LOGGER.warning(
                "Rejecting invalid pending message: %s - %s",
                pending_message.item_hash,
                str(exception),
            )
            return self._handle_rejection(
                session=session, pending_message=pending_message, exception=exception
            )
        else:
            return await self._handle_retry(
                session=session, pending_message=pending_message, exception=exception
            )
