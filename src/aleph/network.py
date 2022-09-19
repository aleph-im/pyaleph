import json
import logging
from typing import Coroutine, Dict, List
from urllib.parse import unquote

from p2pclient import Client as P2PClient

from aleph.exceptions import InvalidMessageError
from aleph.register_chain import VERIFIER_REGISTER
from aleph.schemas.pending_messages import BasePendingMessage, parse_message
from aleph.services.ipfs.pubsub import incoming_channel as incoming_ipfs_channel

LOGGER = logging.getLogger("NETWORK")


INCOMING_MESSAGE_AUTHORIZED_FIELDS = [
    "item_hash",
    "item_content",
    "item_type",
    "chain",
    "channel",
    "sender",
    "type",
    "time",
    "signature",
]


async def get_pubsub_message(ipfs_pubsub_message: Dict) -> BasePendingMessage:
    """
    Extracts an Aleph message out of a pubsub message.

    Note: this function validates the format of the message, but does not
    perform extra validation (ex: signature checks).
    """
    message_data = ipfs_pubsub_message.get("data", b"").decode("utf-8")

    try:
        message_dict = json.loads(unquote(message_data))
    except json.JSONDecodeError:
        raise InvalidMessageError(
            "Data is not JSON: {}".format(ipfs_pubsub_message.get("data", ""))
        )

    LOGGER.debug("New message! %r" % message_dict)

    message = parse_message(message_dict)
    return message


async def verify_signature(message: BasePendingMessage) -> None:
    chain = message.chain
    signer = VERIFIER_REGISTER.get(chain, None)
    if signer is None:
        raise InvalidMessageError("Unknown chain for validation %r" % chain)
    try:
        if await signer(message):
            return
        else:
            raise InvalidMessageError("The signature of the message is invalid")
    except ValueError:
        raise InvalidMessageError("Signature validation error")


def listener_tasks(config, p2p_client: P2PClient) -> List[Coroutine]:
    from aleph.services.p2p.protocol import incoming_channel as incoming_p2p_channel

    # for now (1st milestone), we only listen on a single global topic...
    tasks: List[Coroutine] = [incoming_p2p_channel(p2p_client, config.aleph.queue_topic.value)]
    if config.ipfs.enabled.value:
        tasks.append(incoming_ipfs_channel(config.aleph.queue_topic.value))
    return tasks
