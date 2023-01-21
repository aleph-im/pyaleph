import logging

from aleph.schemas.pending_messages import (
    BasePendingMessage,
)

LOGGER = logging.getLogger("chains.common")


def get_verification_buffer(message: BasePendingMessage) -> bytes:
    """
    Returns the serialized string that was signed by the user when sending an Aleph message.
    """
    buffer = f"{message.chain.value}\n{message.sender}\n{message.type.value}\n{message.item_hash}"
    return buffer.encode("utf-8")
