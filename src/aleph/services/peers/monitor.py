import asyncio
import json
import logging
from urllib.parse import unquote

from aleph_p2p_client import AlephP2PServiceClient

from aleph.services.ipfs.pubsub import sub as sub_ipfs
from aleph.types.protocol import Protocol

LOGGER = logging.getLogger("P2P.peers")


async def handle_incoming_host(
    data: bytes, sender: str, source: Protocol = Protocol.P2P
):
    from aleph.model.p2p import add_peer

    try:
        LOGGER.debug("New message received from %s", sender)
        message_data = data.decode("utf-8")
        content = json.loads(unquote(message_data))

        # TODO: replace this validation by marshaling (ex: Pydantic)
        peer_type = content.get("peer_type", "P2P")
        if not isinstance(content["address"], str):
            raise ValueError("Bad address")
        if not isinstance(content["peer_type"], str):
            raise ValueError("Bad peer type")

        # TODO: handle interests and save it

        if peer_type not in ["P2P", "HTTP", "IPFS"]:
            raise ValueError("Unsupported peer type %r" % peer_type)

        await add_peer(
            address=content["address"],
            peer_type=peer_type,
            source=source,
            sender=sender,
        )
    except Exception as e:
        if isinstance(e, ValueError):
            LOGGER.info("Received a bad peer info %s from %s" % (e.args[0], sender))
        else:
            LOGGER.exception("Exception in pubsub peers monitoring")


async def monitor_hosts_p2p(
    p2p_client: AlephP2PServiceClient, alive_topic: str
) -> None:
    while True:
        try:
            await p2p_client.subscribe(alive_topic)
            async for alive_message in p2p_client.receive_messages(alive_topic):
                protocol, topic, peer_id = alive_message.routing_key.split(".")
                await handle_incoming_host(
                    data=alive_message.body, sender=peer_id, source=Protocol.P2P
                )

        except Exception:
            LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")

        await asyncio.sleep(2)


async def monitor_hosts_ipfs(alive_topic: str):
    while True:
        try:
            async for message in sub_ipfs(alive_topic):
                await handle_incoming_host(
                    data=message["data"], sender=message["from"], source=Protocol.IPFS
                )
        except Exception:
            LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")
