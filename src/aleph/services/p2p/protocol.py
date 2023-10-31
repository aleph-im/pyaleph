import asyncio
import logging

from aleph_p2p_client import AlephP2PServiceClient

from aleph.handlers.message_handler import MessageHandler, MessagePublisher
from aleph.network import decode_pubsub_message
from aleph.toolkit.timestamp import utc_now
from aleph.types.message_status import InvalidMessageException

LOGGER = logging.getLogger(__name__)


async def incoming_channel(
    p2p_client: AlephP2PServiceClient, topic: str, message_publisher: MessagePublisher
) -> None:
    LOGGER.debug("incoming channel started...")

    await p2p_client.subscribe(topic)

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

                    # we should check the sender here to avoid spam
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

                    await message_publisher.add_pending_message(
                        message_dict=message_dict, reception_time=utc_now()
                    )
                except Exception:
                    LOGGER.exception("Can't handle message")

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")

        await asyncio.sleep(2)
