import asyncio
import logging

from aleph_p2p_client import AlephP2PServiceClient

from aleph.exceptions import InvalidMessageError
from aleph.network import decode_pubsub_message

LOGGER = logging.getLogger("P2P.protocol")


async def incoming_channel(p2p_client: AlephP2PServiceClient, topic: str) -> None:
    LOGGER.debug("incoming channel started...")
    from aleph.chains.common import delayed_incoming

    await p2p_client.subscribe(topic)

    while True:
        try:
            async for message in p2p_client.receive_messages(topic):
                try:
                    protocol, topic, peer_id = message.routing_key.split(".")
                    LOGGER.debug("Received new %s message on topic %s from %s", protocol, topic, peer_id)

                    # we should check the sender here to avoid spam
                    # and such things...
                    try:
                        message = await decode_pubsub_message(message.body)
                    except InvalidMessageError:
                        LOGGER.warning("Received invalid message on P2P topic %s from %s", topic, peer_id)
                        continue

                    LOGGER.debug("New message %r" % message)
                    await delayed_incoming(message)
                except Exception:
                    LOGGER.exception("Can't handle message")

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")

        await asyncio.sleep(2)
