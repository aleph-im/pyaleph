import asyncio
import base64
import logging
from typing import Union

import base58

from aleph.types import InvalidMessageError
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
    from aleph.network import incoming_check
    from aleph.chains.common import incoming

    # When using some deployment strategies such as docker-compose,
    # the IPFS service may not be ready by the time this function
    # is called. This variable define how many connection attempts
    # will not be logged as exceptions.
    trials_before_exception: int = 5
    while True:
        try:
            async for mvalue in sub(topic):
                try:
                    message = await incoming_check(mvalue)
                    LOGGER.debug("New message %r" % message)
                    asyncio.create_task(incoming(message, bulk_operation=False))
                except InvalidMessageError:
                    LOGGER.warning(f"Invalid message {message}")

                # Raise all connection errors after one has succeeded.
                trials_before_exception = 0
        except Exception:
            if trials_before_exception > 0:
                LOGGER.info("Exception in IPFS pubsub, reconnecting in 2 seconds...")
            else:
                LOGGER.exception(
                    "Exception in IPFS pubsub, reconnecting in 2 seconds..."
                )
            await asyncio.sleep(2)
        finally:
            trials_before_exception = max(trials_before_exception - 1, 0)
