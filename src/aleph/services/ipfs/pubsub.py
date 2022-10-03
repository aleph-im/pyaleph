import asyncio
import logging

from aleph.exceptions import InvalidMessageError
from .service import IpfsService

LOGGER = logging.getLogger("IPFS.PUBSUB")


# TODO: add type hint for message_processor, it currently causes a cyclical import
async def incoming_channel(ipfs_service: IpfsService, topic: str, message_processor) -> None:
    from aleph.network import decode_pubsub_message

    while True:
        try:
            async for mvalue in ipfs_service.sub(topic):
                try:
                    message = await decode_pubsub_message(mvalue["data"])
                    LOGGER.debug("New message %r" % message)
                    asyncio.create_task(message_processor.process_one_message(message))
                except InvalidMessageError:
                    LOGGER.warning(f"Invalid message {mvalue}")
        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting in 2 seconds...")
            await asyncio.sleep(2)
