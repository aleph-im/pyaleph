import asyncio
import logging
from collections import OrderedDict
from typing import Any

from aleph_p2p_client import AlephP2PServiceClient

from aleph.handlers.message_handler import MessagePublisher
from aleph.network import decode_pubsub_message
from aleph.toolkit.timestamp import utc_now
from aleph.types.message_status import InvalidMessageException

LOGGER = logging.getLogger(__name__)

# Max entries kept in the per-topic dedup cache before FIFO eviction.
SEEN_HASHES_MAXLEN = 200_000


async def incoming_channel(
    p2p_client: AlephP2PServiceClient, topic: str, message_publisher: MessagePublisher
) -> None:
    LOGGER.debug("incoming channel started...")

    await p2p_client.subscribe(topic)
    # Dedup cache: OrderedDict used as a bounded set — O(1) membership with
    # FIFO eviction. A deque here made the per-message membership check an
    # O(n) linear scan (up to SEEN_HASHES_MAXLEN) on the hot ingress path.
    seen_hashes: "OrderedDict[tuple[Any, Any, Any], None]" = OrderedDict()

    while True:
        try:
            async for message in p2p_client.receive_messages(topic):
                try:
                    protocol, topic, peer_id = message.routing_key.split(".")
                    LOGGER.debug(
                        "Received new %s message on topic %s from %s",
                        protocol,
                        topic,
                        peer_id,
                    )

                    # We should check the sender here to avoid spam
                    # and such things...
                    try:
                        message_dict = await decode_pubsub_message(message.body)
                    except InvalidMessageException:
                        LOGGER.warning(
                            "Received invalid message on P2P topic %s from %s",
                            topic,
                            peer_id,
                        )
                        continue

                    # In-memory cache to avoid handling the same message twice.
                    key = (
                        message_dict["sender"],
                        message_dict["item_hash"],
                        message_dict["signature"],
                    )
                    if key in seen_hashes:
                        # Messages are already ACKed by
                        # p2p_client.receive_messages() when the process is
                        # healthy.
                        continue

                    seen_hashes[key] = None
                    if len(seen_hashes) > SEEN_HASHES_MAXLEN:
                        seen_hashes.popitem(last=False)  # evict oldest (FIFO)

                    await message_publisher.add_pending_message(
                        message_dict=message_dict, reception_time=utc_now()
                    )
                except Exception:
                    LOGGER.exception("Can't handle message")

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")

        await asyncio.sleep(0.1)
