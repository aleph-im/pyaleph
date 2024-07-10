import datetime as dt
import itertools
import json
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Union

from aleph_message.models import ItemType, MessageConfirmation

from aleph.db.models import MessageDb, MessageStatusDb, PendingMessageDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession
from aleph.types.message_processing_result import MessageProcessingResult
from aleph.types.message_status import MessageStatus


def make_validated_message_from_dict(
    message_dict: Mapping[str, Any],
    raw_content: Optional[Union[str, bytes]] = None,
    confirmations: Optional[List[MessageConfirmation]] = None,
) -> MessageDb:
    """
    Creates a validated message instance from a raw message dictionary.
    This is helpful to easily import fixtures from an API or the DB and transform
    them into a valid object.

    :param message_dict: The raw message dictionary.
    :param raw_content: The raw content of the message, as a string or bytes.
    :param confirmations: List of confirmations, if any.
    """

    if raw_content is None:
        assert message_dict["item_type"] == ItemType.inline
        raw_content = message_dict["item_content"]

    pending_message = PendingMessageDb.from_message_dict(
        message_dict, fetched=True, reception_time=dt.datetime(2022, 1, 1)
    )
    return MessageDb.from_pending_message(
        pending_message=pending_message,
        content_dict=json.loads(raw_content),
        content_size=len(raw_content),
    )


async def process_pending_messages(
    message_processor: PendingMessageProcessor,
    pending_messages: Sequence[PendingMessageDb],
    session: DbSession,
) -> Iterable[MessageProcessingResult]:

    for pending_message in pending_messages:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=utc_now(),
            )
        )
    session.add_all(pending_messages)
    session.commit()

    pipeline = message_processor.make_pipeline()

    return itertools.chain.from_iterable([results async for results in pipeline])
