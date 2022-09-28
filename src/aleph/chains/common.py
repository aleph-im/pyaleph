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


async def mark_confirmed_data(chain_name, tx_hash, height):
    """Returns data required to mark a particular hash as confirmed
    in underlying chain.
    """
    return {
        "confirmed": True,
        "confirmations": [  # TODO: we should add the current one there
            # and not replace it.
            {"chain": chain_name, "height": height, "hash": tx_hash}
        ],
    }
