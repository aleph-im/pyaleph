import asyncio
import json
import logging
from typing import Coroutine, Dict, List, Optional
from urllib.parse import unquote

from p2pclient import Client as P2PClient

from aleph.register_chain import VERIFIER_REGISTER
from aleph.services.ipfs.pubsub import incoming_channel as incoming_ipfs_channel
from aleph.types import ItemType, InvalidMessageError
from aleph.utils import get_sha256

LOGGER = logging.getLogger("NETWORK")

MAX_INLINE_SIZE = 200000  # 200kb max inline content size.

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

HOST = None


async def incoming_check(ipfs_pubsub_message):
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
    message: Dict,
    from_chain: bool = False,
    from_network: bool = False,
    trusted: bool = False,
) -> Optional[Dict]:
    """This function should check the incoming message and verify any
    extraneous or dangerous information for the rest of the process.
    It also checks the data hash if it's not done by an external provider (ipfs)
    and the data length.
    Example of dangerous data: fake confirmations, fake tx_hash, bad times...

    If a item_content is there, set the item_type to inline, else to ipfs (default).

    TODO: Implement it fully! Dangerous!
    """
    if not isinstance(message, dict):
        raise InvalidMessageError("Message must be a dict")

    if not message:
        raise InvalidMessageError("Message must not be empty")

    if "item_hash" not in message:
        raise InvalidMessageError("Missing field 'item_hash' in message")
    for field in ("chain", "sender", "signature"):
        if field not in message:
            raise InvalidMessageError(
                f"Missing field '{field}' in message {message['item_hash']}"
            )

    if not isinstance(message["item_hash"], str):
        raise InvalidMessageError("Unknown hash %s" % message["item_hash"])

    if not isinstance(message["chain"], str):
        raise InvalidMessageError("Unknown chain %s" % message["chain"])

    if message.get("channel", None) is not None:
        if not isinstance(message.get("channel", None), str):
            raise InvalidMessageError("Unknown channel %s" % message["channel"])

    if not isinstance(message["sender"], str):
        raise InvalidMessageError("Unknown sender %s" % message["sender"])

    if not isinstance(message["signature"], str):
        raise InvalidMessageError("Unknown signature %s" % message["signature"])

    if message.get("item_content", None) is not None:
        if len(message["item_content"]) > MAX_INLINE_SIZE:
            raise InvalidMessageError("Message too long")
        await asyncio.sleep(0)

        if message.get("hash_type", "sha256") == "sha256":  # leave the door open.
            if not trusted:
                loop = asyncio.get_event_loop()
                item_hash = get_sha256(message["item_content"])

                if message["item_hash"] != item_hash:
                    raise InvalidMessageError("Bad hash")
        else:
            raise InvalidMessageError("Unknown hash type %s" % message["hash_type"])

        message["item_type"] = ItemType.Inline.value

    else:
        try:
            message["item_type"] = ItemType.from_hash(message["item_hash"]).value
        except ValueError as error:
            LOGGER.warning(error)

    if trusted:
        # only in the case of a message programmatically built here
        # from legacy native chain signing for example (signing offloaded)
        return message
    else:
        message = {
            k: v for k, v in message.items() if k in INCOMING_MESSAGE_AUTHORIZED_FIELDS
        }
        await asyncio.sleep(0)
        chain = message.get("chain", None)
        signer = VERIFIER_REGISTER.get(chain, None)
        if signer is None:
            raise InvalidMessageError("Unknown chain for validation %r" % chain)
        try:
            if await signer(message):
                return message
        except ValueError:
            raise InvalidMessageError("Signature validation error")

        return None


def listener_tasks(config, p2p_client: P2PClient) -> List[Coroutine]:
    from aleph.services.p2p.protocol import incoming_channel as incoming_p2p_channel

    # for now (1st milestone), we only listen on a single global topic...
    tasks: List[Coroutine] = [incoming_p2p_channel(p2p_client, config.aleph.queue_topic.value)]
    if config.ipfs.enabled.value:
        tasks.append(incoming_ipfs_channel(config.aleph.queue_topic.value))
    return tasks
