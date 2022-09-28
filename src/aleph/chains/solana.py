import json
import logging

import base58
from nacl.signing import VerifyKey

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage
from .connector import Verifier

LOGGER = logging.getLogger("chains.solana")
CHAIN_NAME = "SOL"


class SolanaConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        try:
            signature = json.loads(message.signature)
            sigdata = base58.b58decode(signature["signature"])
            public_key = base58.b58decode(signature["publicKey"])
        except Exception:
            LOGGER.exception("Solana signature deserialization error")
            return False

        try:
            if signature.get("version", 1) != 1:
                LOGGER.warning(
                    "Unsupported signature version %d" % signature.get("version")
                )
        except Exception:
            LOGGER.exception("Solana signature version error")
            return False

        if message.sender != signature["publicKey"]:
            LOGGER.exception("Solana signature source error")
            return False

        try:
            verify_key = VerifyKey(public_key)
            verification_buffer = get_verification_buffer(message)
            verif = verify_key.verify(verification_buffer, signature=sigdata)
            result = verif == verification_buffer
        except Exception:
            LOGGER.exception("Solana Signature verification error")
            result = False

        return result
