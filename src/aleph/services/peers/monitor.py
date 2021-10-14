import json

from urllib.parse import unquote
from aleph.services.peers.common import ALIVE_TOPIC, IPFS_ALIVE_TOPIC
from aleph.services.p2p.pubsub import decode_msg
from aleph.services.ipfs.common import get_base_url
from aleph.services.ipfs.pubsub import sub as sub_ipfs

import logging

from aleph.types import ItemType, Protocol

LOGGER = logging.getLogger("P2P.peers")


async def handle_incoming_host(mvalue, source: Protocol=Protocol.P2P):
    from aleph.model.p2p import add_peer

    try:
        LOGGER.debug("New message received %r" % mvalue)
        message_data = mvalue.get("data", b"").decode("utf-8")
        content = json.loads(unquote(message_data))
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
            sender=mvalue["from"],
        )
    except Exception as e:
        if isinstance(e, ValueError) and mvalue.get("from"):
            LOGGER.info(
                "Received a bad peer info %s from %s" % (e.args[0], mvalue["from"])
            )
        else:
            LOGGER.exception("Exception in pubsub peers monitoring")


async def monitor_hosts_p2p(psub):
    try:
        alive_sub = await psub.subscribe(ALIVE_TOPIC)
        while True:
            mvalue = await alive_sub.get()
            mvalue = await decode_msg(mvalue)
            await handle_incoming_host(mvalue, source=Protocol.P2P)
    except Exception:
        LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")


async def monitor_hosts_ipfs(config):
    try:
        async for mvalue in sub_ipfs(
            IPFS_ALIVE_TOPIC, base_url=await get_base_url(config)
        ):
            await handle_incoming_host(mvalue, source=Protocol.IPFS)
    except Exception:
        LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")
