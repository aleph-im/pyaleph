import json
import logging
from typing import Any, Dict
from urllib.parse import unquote

from p2pclient import Client as P2PClient
from p2pclient.pb.p2pd_pb2 import PSMessage
from p2pclient.utils import read_pbmsg_safe

from aleph.services.ipfs.pubsub import sub as sub_ipfs
from aleph.services.peers.common import ALIVE_TOPIC, IPFS_ALIVE_TOPIC
from aleph.services.utils import pubsub_msg_to_dict
from aleph.types import Protocol

LOGGER = logging.getLogger("P2P.peers")


async def handle_incoming_host(pubsub_msg: Dict[str, Any], source: Protocol = Protocol.P2P):
    from aleph.model.p2p import add_peer

    sender = pubsub_msg["from"]

    try:
        LOGGER.debug("New message received %r" % pubsub_msg)
        message_data = pubsub_msg.get("data", b"").decode("utf-8")
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


async def monitor_hosts_p2p(p2p_client: P2PClient) -> None:

    try:
        stream = await p2p_client.pubsub_subscribe(ALIVE_TOPIC)
        while True:
            pubsub_msg = PSMessage()
            await read_pbmsg_safe(stream, pubsub_msg)
            msg_dict = pubsub_msg_to_dict(pubsub_msg)
            await handle_incoming_host(msg_dict, source=Protocol.P2P)
    except Exception:
        LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")


async def monitor_hosts_ipfs(config):
    try:
        async for mvalue in sub_ipfs(IPFS_ALIVE_TOPIC):
            await handle_incoming_host(mvalue, source=Protocol.IPFS)
    except Exception:
        LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")
