import logging
from typing import AsyncIterator

from p2pclient import Client as P2PClient
from p2pclient.pb.p2pd_pb2 import PSMessage
from p2pclient.utils import read_pbmsg_safe


LOGGER = logging.getLogger("P2P.pubsub")


async def sub(p2p_client: P2PClient, topic: str) -> AsyncIterator[PSMessage]:
    stream = await p2p_client.pubsub_subscribe(topic)
    while True:
        pubsub_msg = PSMessage()
        await read_pbmsg_safe(stream, pubsub_msg)
        LOGGER.debug("New message received %r" % pubsub_msg)

        yield pubsub_msg


async def pub(p2p_client: P2PClient, topic: str, message: str) -> None:
    await p2p_client.pubsub_publish(topic, message.encode("utf-8"))
