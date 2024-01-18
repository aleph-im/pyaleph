import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from aiohttp import web
from aiohttp.web_exceptions import HTTPException
from aleph_message.models import ExecutableContent, ItemHash, MessageType
from dataclasses_json import DataClassJsonMixin

from aleph.db.accessors.messages import get_message_by_item_hash, get_message_status
from aleph.db.models import MessageDb, MessageStatusDb
from aleph.services.cost import compute_cost, compute_flow_cost
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)


# This is not defined in aiohttp.web_exceptions
class HTTPProcessing(HTTPException):
    status_code = 102


# Mapping between message statuses to their corresponding exceptions and messages
MESSAGE_STATUS_EXCEPTIONS = {
    MessageStatusDb.status.PENDING: (HTTPProcessing, "Message still pending"),
    MessageStatusDb.status.REJECTED: (web.HTTPNotFound, "This message was rejected"),
    MessageStatusDb.status.FORGOTTEN: (
        web.HTTPGone,
        "This message has been forgotten",
    ),
}


@dataclass
class MessagePrice(DataClassJsonMixin):
    """Dataclass used to expose message required tokens."""

    required_tokens: Optional[Decimal] = None


async def get_executable_message(session: DbSession, item_hash_str: str) -> MessageDb:
    """Attempt to get an executable message from the database.
    Raises an HTTP exception if the message is not found, not processed or is not an executable message.
    """

    # Parse the item_hash_str into an ItemHash object
    try:
        item_hash = ItemHash(item_hash_str)
    except ValueError:
        raise web.HTTPBadRequest(body=f"Invalid message hash: {item_hash_str}")

    # Get the message status from the database
    message_status_db = get_message_status(session=session, item_hash=item_hash)
    if not message_status_db:
        raise web.HTTPNotFound()
    # Loop through the status_exceptions to find a match and raise the corresponding exception
    if message_status_db.status in MESSAGE_STATUS_EXCEPTIONS:
        exception, message = MESSAGE_STATUS_EXCEPTIONS[message_status_db.status]
        raise exception(body=f"{message}: {item_hash_str}")
    assert message_status_db.status == MessageStatusDb.status.PROCESSED

    # Get the message from the database
    message = get_message_by_item_hash(session, item_hash)
    if not message:
        raise web.HTTPNotFound(body="Message not found, despite appearing as processed")
    if message.type not in (MessageType.instance, MessageType.program):
        raise web.HTTPBadRequest(
            body=f"Message is not an executable message: {item_hash_str}"
        )

    return message


async def message_price(request: web.Request):
    """Returns the price of an executable message."""

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message = await get_executable_message(session, request.match_info["item_hash"])

        content: ExecutableContent = message.parsed_content

        if content.payment and content.payment.is_stream:
            required_tokens = compute_flow_cost(session=session, content=content)
        else:
            required_tokens = compute_cost(session=session, content=content)

    return web.Response(
        text=MessagePrice(required_tokens=required_tokens).to_json(),
        content_type="application/json",
    )
