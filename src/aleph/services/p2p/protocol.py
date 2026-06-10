import asyncio
import logging
from collections import deque
from typing import Any

from aleph.handlers.message_handler import MessagePublisher
from aleph.network import decode_pubsub_message
from aleph.services.p2p.client import P2PGrpcClient
from aleph.toolkit.timestamp import utc_now
from aleph.types.message_status import InvalidMessageException

LOGGER = logging.getLogger(__name__)


async def incoming_channel(
    p2p_client: P2PGrpcClient, topic: str, message_publisher: MessagePublisher
) -> None:
    LOGGER.debug("incoming channel started...")

    seen_hashes: deque[tuple[Any, Any, Any]] = deque([], maxlen=200000)

    while True:
        try:
            async for message in p2p_client.receive_messages(topic):
                try:
                    LOGGER.debug(
                        "Received new message on topic %s from %s",
                        message.topic,
                        message.sender,
                    )

                    try:
                        message_dict = await decode_pubsub_message(message.data)
                        # In-memory cache to avoid processing the same message
                        # several times (the network can deliver duplicates).
                        cache_key = (
                            message_dict["sender"],
                            message_dict["item_hash"],
                            message_dict["signature"],
                        )
                        if cache_key in seen_hashes:
                            continue
                        seen_hashes.append(cache_key)
                    except InvalidMessageException:
                        LOGGER.warning(
                            "Received invalid message on P2P topic %s from %s",
                            message.topic,
                            message.sender,
                        )
                        continue

                    await message_publisher.add_pending_message(
                        message_dict=message_dict, reception_time=utc_now()
                    )
                except Exception:
                    LOGGER.exception("Can't handle message")

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")

        await asyncio.sleep(0.1)
