import logging
from typing import AsyncIterator, Union

from p2pclient import Client as P2PClient
from p2pclient.pb.p2pd_pb2 import PSMessage
from p2pclient.utils import read_pbmsg_safe
import anyio

LOGGER = logging.getLogger("P2P.pubsub")


async def subscribe(p2p_client: P2PClient, topic: str) -> anyio.abc.SocketStream:
    """
    Subscribes to the specified topic.
    :param p2p_client: P2P daemon client.
    :param topic: Topic on which to subscribe.
    :return: A socket stream object. This stream can be used to read data posted by other peers on the topic.
    """
    return await p2p_client.pubsub_subscribe(topic)


async def receive_pubsub_messages(stream: anyio.abc.SocketStream) -> AsyncIterator[PSMessage]:
    """
    Receives messages from a P2P pubsub topic in a loop and yields them one by one.
    :param stream: The stream (= return value of the `subscribe` function) to read data from.
    """
    while True:
        pubsub_msg = PSMessage()
        await read_pbmsg_safe(stream, pubsub_msg)
        LOGGER.debug("New message received %r" % pubsub_msg)

        yield pubsub_msg


async def publish(p2p_client: P2PClient, topic: str, message: Union[bytes, str]) -> None:
    """
    Publishes a message on the specified topic.
    :param p2p_client: P2P daemon client.
    :param topic: Topic on which to send the message.
    :param message: The message itself. Can be provided as bytes or as a string.
    """

    data = message if isinstance(message, bytes) else message.encode("UTF-8")
    await p2p_client.pubsub_publish(topic, data)
