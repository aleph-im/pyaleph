import json
import logging
from typing import Coroutine, List
from urllib.parse import unquote

from aleph_p2p_client import AlephP2PServiceClient

from aleph.chains.chain_service import ChainService
from aleph.handlers.message_handler import MessageHandler
from aleph.exceptions import InvalidMessageError
from aleph.model import make_gridfs_client
from aleph.schemas.pending_messages import BasePendingMessage, parse_message
from aleph.services.ipfs.pubsub import incoming_channel as incoming_ipfs_channel
from aleph.services.storage.gridfs_engine import GridFsStorageEngine
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

    storage_service = StorageService(
        storage_engine=GridFsStorageEngine(gridfs_client=make_gridfs_client())
    )
    chain_service = ChainService(storage_service=storage_service)
    message_processor = MessageHandler(
        chain_service=chain_service, storage_service=storage_service
    )

    # for now (1st milestone), we only listen on a single global topic...
    tasks: List[Coroutine] = [
        incoming_p2p_channel(
            p2p_client, config.aleph.queue_topic.value, message_processor
        )
    ]
    if config.ipfs.enabled.value:
        tasks.append(
            incoming_ipfs_channel(config.aleph.queue_topic.value, message_processor)
        )
    return tasks
