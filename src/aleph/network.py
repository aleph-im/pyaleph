import asyncio
import json
import logging
from typing import Coroutine, Dict, List
from urllib.parse import unquote

from p2pclient import Client as P2PClient

from aleph.exceptions import InvalidMessageError
from aleph.register_chain import VERIFIER_REGISTER
from aleph.schemas.pending_messages import parse_message
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


async def incoming_check(ipfs_pubsub_message: Dict) -> Dict:
    """Verifies an incoming message is sane, protecting from spam in the
    meantime.

    TODO: actually implement this, no check done here yet. IMPORTANT.
    """

    try:
        message_data = ipfs_pubsub_message.get("data", b"").decode("utf-8")
        message = json.loads(unquote(message_data))
        LOGGER.debug("New message! %r" % message)

        if message is None:
            raise InvalidMessageError("Message may not be None")

        message = await check_message(message, from_network=True)
        return message
    except json.JSONDecodeError:
        raise InvalidMessageError(
            "Data is not JSON: {}".format(ipfs_pubsub_message.get("data", ""))
        )


async def check_message(
    message_dict: Dict,
    from_chain: bool = False,
    from_network: bool = False,
    trusted: bool = False,
) -> Dict:
    """This function should check the incoming message and verify any
    extraneous or dangerous information for the rest of the process.
    It also checks the data hash if it's not done by an external provider (ipfs)
    and the data length.
    Example of dangerous data: fake confirmations, fake tx_hash, bad times...

    If a item_content is there, set the item_type to inline, else to ipfs (default).

    TODO: Implement it fully! Dangerous!
    """

    message = parse_message(message_dict)

    # TODO: this is a temporary fix to set the item_type of the message to the correct
    #       value. This should be replaced by a full use of Pydantic models.
    message_dict["item_type"] = message.item_type.value

    if trusted:
        # only in the case of a message programmatically built here
        # from legacy native chain signing for example (signing offloaded)
        return message_dict
    else:
        chain = message.chain
        signer = VERIFIER_REGISTER.get(chain, None)
        if signer is None:
            raise InvalidMessageError("Unknown chain for validation %r" % chain)
        try:
            if await signer(message):
                return message_dict
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
