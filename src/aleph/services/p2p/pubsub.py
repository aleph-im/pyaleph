import logging
from typing import AsyncIterator

from p2pclient.pb.p2pd_pb2 import PSMessage
from p2pclient.utils import read_pbmsg_safe

from .singleton import get_p2p_client

LOGGER = logging.getLogger("P2P.pubsub")


async def sub(topic: str) -> AsyncIterator[PSMessage]:
    p2p_client = get_p2p_client()
    stream = await p2p_client.pubsub_subscribe(topic)
    while True:
        pubsub_msg = PSMessage()
        await read_pbmsg_safe(stream, pubsub_msg)
        LOGGER.debug("New message received %r" % pubsub_msg)

        yield pubsub_msg


async def pub(topic: str, message: str) -> None:
    p2p_client = get_p2p_client()
    await p2p_client.pubsub_publish(topic, message.encode("utf-8"))
