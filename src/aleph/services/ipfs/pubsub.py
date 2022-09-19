import asyncio
import base64
import logging
from typing import Union

import base58

from ...exceptions import InvalidMessageError
from .common import get_ipfs_api

LOGGER = logging.getLogger("IPFS.PUBSUB")


async def decode_msg(msg):
    return {
        "from": base58.b58encode(base64.b64decode(msg["from"])),
        "data": base64.b64decode(msg["data"]),
        "seqno": base58.b58encode(base64.b64decode(msg["seqno"])),
        "topicIDs": msg["topicIDs"],
    }


async def sub(topic: str):
    api = await get_ipfs_api()

    async for mvalue in api.pubsub.sub(topic):
        try:
            LOGGER.debug("New message received %r" % mvalue)

            # we should check the sender here to avoid spam
            # and such things...
            yield mvalue

        except Exception:
            LOGGER.exception("Error handling message")


async def pub(topic: str, message: Union[str, bytes]):
    api = await get_ipfs_api()
    await api.pubsub.pub(topic, message)


async def incoming_channel(topic) -> None:
    from aleph.network import get_pubsub_message
    from aleph.chains.common import process_one_message

    while True:
        try:
            async for mvalue in sub(topic):
                try:
                    message = await get_pubsub_message(mvalue)
                    LOGGER.debug("New message %r" % message)
                    asyncio.create_task(process_one_message(message))
                except InvalidMessageError:
                    LOGGER.warning(f"Invalid message {mvalue}")
        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting in 2 seconds...")
            await asyncio.sleep(2)
