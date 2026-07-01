import logging
from typing import Union

from aleph.services.p2p.client import P2PGrpcClient

LOGGER = logging.getLogger("P2P.pubsub")


async def publish(
    p2p_client: P2PGrpcClient,
    topic: str,
    message: Union[bytes, str],
    loopback: bool = False,
) -> None:
    """
    Publishes a message on the specified topic.
    :param p2p_client: P2P service client.
    :param topic: Topic on which to send the message.
    :param message: The message itself. Can be provided as bytes or as a string.
    :param loopback: Whether the message should also be delivered to local
                     subscribers (the service echoes it on Subscribe streams).
    """

    data = message if isinstance(message, bytes) else message.encode("UTF-8")
    await p2p_client.publish(data=data, topic=topic, echo=loopback)
