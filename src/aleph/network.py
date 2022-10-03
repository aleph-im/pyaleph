import json
import logging
from typing import Coroutine, List
from urllib.parse import unquote

from aleph_p2p_client import AlephP2PServiceClient

from aleph.chains.chain_service import ChainService
from aleph.exceptions import InvalidMessageError
from aleph.handlers.message_handler import MessageHandler
from aleph.schemas.pending_messages import BasePendingMessage, parse_message
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.ipfs.pubsub import incoming_channel as incoming_ipfs_channel
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService

LOGGER = logging.getLogger("NETWORK")


async def decode_pubsub_message(message_data: bytes) -> BasePendingMessage:
    """
    Extracts an Aleph message out of a pubsub message.

    Note: this function validates the format of the message, but does not
    perform extra validation (ex: signature checks).
    """
    try:
        message_dict = json.loads(unquote(message_data.decode("utf-8")))
    except json.JSONDecodeError:
        raise InvalidMessageError("Data is not JSON: {!r}".format(message_data))

    LOGGER.debug("New message! %r" % message_dict)

    message = parse_message(message_dict)
    return message


def listener_tasks(config, p2p_client: AlephP2PServiceClient) -> List[Coroutine]:
    from aleph.services.p2p.protocol import incoming_channel as incoming_p2p_channel

    # TODO: these should be passed as parameters. This module could probably be a class instead?
    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
    )
    chain_service = ChainService(storage_service=storage_service)
    message_processor = MessageHandler(
        chain_service=chain_service, storage_service=storage_service
    )

    # for now (1st milestone), we only listen on a single global topic...
    tasks: List[Coroutine] = [
        incoming_p2p_channel(
            p2p_client=p2p_client,
            topic=config.aleph.queue_topic.value,
            message_processor=message_processor,
        )
    ]
    if config.ipfs.enabled.value:
        tasks.append(
            incoming_ipfs_channel(
                ipfs_service=ipfs_service,
                topic=config.aleph.queue_topic.value,
                message_processor=message_processor,
            )
        )
    return tasks
