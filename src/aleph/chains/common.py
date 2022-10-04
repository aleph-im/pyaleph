import logging

from aleph.schemas.pending_messages import (
    BasePendingMessage,
)

LOGGER = logging.getLogger("chains.common")


def get_verification_buffer(message: BasePendingMessage) -> bytes:
    """Returns a serialized string to verify the message integrity
    (this is was it signed)
    """
    buffer = f"{message.chain}\n{message.sender}\n{message.type}\n{message.item_hash}"
    return buffer.encode("utf-8")
