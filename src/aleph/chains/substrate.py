import json
import logging

from substrateinterface import Keypair

from aleph.chains.common import get_verification_buffer
from aleph.register_chain import register_verifier
from aleph.schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger("chains.substrate")
CHAIN_NAME = "DOT"


async def verify_signature(message: BasePendingMessage) -> bool:
    """Verifies a signature of a message, return True if verified, false if not"""

    try:
        signature = json.loads(message.signature)
    except Exception:
        LOGGER.exception("Substrate signature deserialization error")
        return False

    try:
        if signature.get("curve", "sr25519") != "sr25519":
            LOGGER.warning("Unsupported curve %s" % signature.get("curve"))
    except Exception:
        LOGGER.exception("Substrate signature Key error")
        return False

    try:
        keypair = Keypair(ss58_address=message.sender)
        verif = (get_verification_buffer(message)).decode("utf-8")
        result = keypair.verify(verif, signature["data"])
    except Exception:
        LOGGER.exception("Substrate Signature verification error")
        result = False

    return result


register_verifier(CHAIN_NAME, verify_signature)
