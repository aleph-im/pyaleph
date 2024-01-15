import logging
from decimal import Decimal
from typing import Optional

from aiohttp import web
from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin

from aleph_message.models import ItemHash, MessageType
from aleph.db.accessors.messages import (
    get_message_status, get_message_by_item_hash,
)
from aleph.services.cost import compute_cost, compute_flow_cost
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


@dataclass
class MessagePrice(DataClassJsonMixin):
    """Dataclass used to expose message required tokens."""
    required_tokens: Optional[Decimal] = None


async def message_price(request: web.Request):
    item_hash_str = request.match_info.get("item_hash")
    try:
        item_hash = ItemHash(item_hash_str)
    except ValueError:
        raise web.HTTPUnprocessableEntity(body=f"Invalid message hash: {item_hash_str}")

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status_db = get_message_status(session=session, item_hash=item_hash)
        if message_status_db is None:
            raise web.HTTPNotFound()
        message = get_message_by_item_hash(session, item_hash)

        if not message or message.type != (MessageType.instance or MessageType.program):
            raise web.HTTPUnprocessableEntity(
                body=f"Invalid message hash: {item_hash_str}"
            )

        content = message.parsed_content

        if content.payment and content.payment.is_stream:
            required_tokens = compute_flow_cost(session=session, content=content)
        else:
            required_tokens = compute_cost(session=session, content=content)

    return web.Response(
        text=MessagePrice(required_tokens=required_tokens).to_json(),
        content_type="application/json",
    )
