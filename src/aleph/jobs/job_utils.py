import asyncio
import datetime as dt
import logging
from typing import Dict, Union, Protocol
from typing import Tuple

from configmanager import Config
from sqlalchemy import update

import aleph.config
from aleph.db.accessors.messages import reject_existing_pending_message
from aleph.db.accessors.pending_messages import set_next_retry
from aleph.db.models import PendingMessageDb, MessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import (
    ErrorCode,
    RetryMessageException,
    FileNotFoundException,
    InvalidMessageException,
    MessageProcessingStatus,
)

LOGGER = logging.getLogger(__name__)


class MessageProcessingResult(Protocol):
    status: MessageProcessingStatus

    @property
    def item_hash(self) -> str:
        pass


class ProcessedMessage(MessageProcessingResult):
    def __init__(self, message: MessageDb, is_confirmation: bool = False):
        self.message = message
        self.status = (
            MessageProcessingStatus.PROCESSED_CONFIRMATION
            if is_confirmation
            else MessageProcessingStatus.PROCESSED_NEW_MESSAGE
        )

    @property
    def item_hash(self) -> str:
        return self.message.item_hash


class FailedMessage(MessageProcessingResult):
    status = MessageProcessingStatus.FAILED_WILL_RETRY

    def __init__(
        self, pending_message: PendingMessageDb, error_code: ErrorCode, will_retry: bool
    ):
        self.pending_message = pending_message
        self.error_code = error_code

        self.status = (
            MessageProcessingStatus.FAILED_WILL_RETRY
            if will_retry
            else MessageProcessingStatus.FAILED_REJECTED
        )

    @property
    def item_hash(self) -> str:
        return self.pending_message.item_hash


class WillRetryMessage(FailedMessage):
    def __init__(self, pending_message: PendingMessageDb, error_code: ErrorCode):
        super().__init__(pending_message, error_code, will_retry=True)


class RejectedMessage(FailedMessage):
    def __init__(self, pending_message: PendingMessageDb, error_code: ErrorCode):
        super().__init__(pending_message, error_code, will_retry=False)


MAX_RETRY_INTERVAL: int = 300


def compute_next_retry_interval(attempts: int) -> dt.timedelta:
    """
    Computes the time interval for the next attempt/retry of a message.

    The interval is computed as 2^attempts and is capped at 5 minutes.
    The maximum amount of retries is controlled in the node configuration.

    :param attempts: Current number of attempts.
    :return: The time interval between the previous processing attempt and the next one.
    """

    seconds = 2 ** attempts
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


class MessageJob:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_handler: MessageHandler,
        max_retries: int,
    ):
        self.session_factory = session_factory
        self.message_handler = message_handler
        self.max_retries = max_retries

    def _handle_rejection(
        self,
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

    def _handle_retry(
        self,
        session: DbSession,
        pending_message: PendingMessageDb,
        exception: BaseException,
    ) -> Union[RejectedMessage, WillRetryMessage]:
        if isinstance(exception, FileNotFoundException):
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
        elif isinstance(exception, RetryMessageException):
            LOGGER.warning(
                "%s error (%d) - message %s marked for retry",
                exception.error_code.name,
                exception.error_code.value,
                pending_message.item_hash,
            )
            error_code = exception.error_code
            schedule_next_attempt(session=session, pending_message=pending_message)
        else:
            LOGGER.exception(
                "Unexpected error while fetching message", exc_info=exception
            )
            error_code = ErrorCode.INTERNAL_ERROR
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
            return self._handle_retry(
                session=session, pending_message=pending_message, exception=exception
            )
