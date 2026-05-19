import logging

from aleph.chains.abc import SignableMessage

LOGGER = logging.getLogger("chains.common")


def get_verification_buffer(message: SignableMessage) -> bytes:
    """
    Returns the serialized string that was signed by the user when sending an Aleph message.
    """
    buffer = f"{message.chain.value}\n{message.sender}\n{message.type.value}\n{message.item_hash}"
    return buffer.encode("utf-8")
