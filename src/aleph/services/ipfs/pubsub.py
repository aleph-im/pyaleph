import asyncio
import logging

from aleph.toolkit.timestamp import utc_now
from aleph.types.message_status import InvalidMessageException

from .service import IpfsService

LOGGER = logging.getLogger(__name__)


# TODO: add type hint for message_processor, it currently causes a cyclical import
async def incoming_channel(
    ipfs_service: IpfsService, topic: str, message_publisher
) -> None:
    from aleph.network import decode_pubsub_message

    while True:
        try:
            async for mvalue in ipfs_service.sub(topic):
                try:
                    message_dict = await decode_pubsub_message(mvalue["data"])
                    await message_publisher.add_pending_message(
                        message_dict=message_dict, reception_time=utc_now()
                    )
                except InvalidMessageException:
                    LOGGER.warning(f"Invalid message {mvalue}")
        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting in 2 seconds...")
            await asyncio.sleep(2)
