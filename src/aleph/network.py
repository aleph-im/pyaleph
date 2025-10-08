import logging
from typing import Any, Coroutine, Dict, List
from urllib.parse import unquote

import aio_pika.abc
from aleph_p2p_client import AlephP2PServiceClient

import aleph.toolkit.json as aleph_json
from aleph.handlers.message_handler import MessagePublisher
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_p2p_client
from aleph.services.ipfs.pubsub import incoming_channel as incoming_ipfs_channel
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import InvalidMessageFormat

LOGGER = logging.getLogger(__name__)


async def decode_pubsub_message(message_data: bytes) -> Dict[str, Any]:
    """
    Extracts an Aleph message out of a pubsub message.

    Note: this function validates the format of the message, but does not
    perform extra validation (ex: signature checks).
    """
    try:
        message_dict = aleph_json.loads(unquote(message_data.decode("utf-8")))
    except aleph_json.DecodeError:
        raise InvalidMessageFormat("Data is not JSON: {!r}".format(message_data))

    LOGGER.debug("New message! %r" % message_dict)

    return message_dict


async def listener_tasks(
    config,
    session_factory: DbSessionFactory,
    node_cache: NodeCache,
    p2p_client: AlephP2PServiceClient,
    mq_channel: aio_pika.abc.AbstractChannel,
) -> List[Coroutine]:
    from aleph.services.p2p.protocol import incoming_channel as incoming_p2p_channel

    # TODO: these should be passed as parameters. This module could probably be a class instead?
    ipfs_client = make_ipfs_p2p_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
        node_cache=node_cache,
    )
    pending_message_exchange = await mq_channel.declare_exchange(
        name=config.rabbitmq.pending_message_exchange.value,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    message_publisher = MessagePublisher(
        session_factory=session_factory,
        storage_service=storage_service,
        config=config,
        pending_message_exchange=pending_message_exchange,
    )

    # for now (1st milestone), we only listen on a single global topic...
    tasks: List[Coroutine] = [
        incoming_p2p_channel(
            p2p_client=p2p_client,
            topic=config.aleph.queue_topic.value,
            message_publisher=message_publisher,
        )
    ]
    if config.ipfs.enabled.value:
        tasks.append(
            incoming_ipfs_channel(
                ipfs_service=ipfs_service,
                topic=config.aleph.queue_topic.value,
                message_publisher=message_publisher,
            )
        )
    return tasks
