import json
import logging

from substrateinterface import Keypair

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage

from .abc import Verifier

LOGGER = logging.getLogger("chains.substrate")


class SubstrateConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        if message.signature is None:
            LOGGER.warning("'%s': missing signature.", message.item_hash)
            return False

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
