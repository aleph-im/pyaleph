import json
from typing import Dict, List, Optional, Union

from aleph_message.models import ItemType, MessageConfirmation

from aleph.schemas.message_content import MessageContent, ContentSource
from aleph.schemas.pending_messages import parse_message
from aleph.schemas.validated_message import validate_pending_message


def make_validated_message_from_dict(
    message_dict: Dict,
    raw_content: Optional[Union[str, bytes]] = None,
    confirmations: Optional[List[MessageConfirmation]] = None,
):
    """
    Creates a validated message instance from a raw message dictionary.
    This is helpful to easily import fixtures from an API or the DB and transform
    them into a valid object.

    :param message_dict: The raw message dictionary.
    :param raw_content: The raw content of the message, as a string or bytes.
    :param confirmations: List of confirmations, if any.
    """

    pending_message = parse_message(message_dict)

    if raw_content is None:
        assert message_dict["item_type"] == ItemType.inline
        raw_content = message_dict["item_content"]

    content_source = (
        ContentSource.INLINE
        if pending_message.item_type == ItemType.inline
        else ContentSource.P2P
    )

    message_content = MessageContent(
        hash=pending_message.item_hash,
        source=content_source,
        value=json.loads(raw_content),
        raw_value=raw_content,
    )

    return validate_pending_message(
        pending_message=pending_message,
        content=message_content,
        confirmations=confirmations or [],
    )
