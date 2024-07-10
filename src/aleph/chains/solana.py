import json
import logging

import base58
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage

from .abc import Verifier

LOGGER = logging.getLogger("chains.solana")
CHAIN_NAME = "SOL"


class SolanaConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        if message.signature is None:
            LOGGER.warning("'%s': missing signature.", message.item_hash)
            return False

        try:
            signature = json.loads(message.signature)
            sigdata = base58.b58decode(signature["signature"])
            public_key = base58.b58decode(signature["publicKey"])
        except ValueError:
            LOGGER.warning("Solana signature deserialization error")
            return False

        if signature.get("version", 1) != 1:
            LOGGER.warning(
                "Unsupported signature version %s" % signature.get("version")
            )
            return False

        if message.sender != signature["publicKey"]:
            LOGGER.warning("Solana signature source error")
            return False

        try:
            verify_key = VerifyKey(public_key)
            verification_buffer = get_verification_buffer(message)
            verif = verify_key.verify(verification_buffer, signature=sigdata)
            result = verif == verification_buffer
        except BadSignatureError:
            result = False
        except Exception:
            LOGGER.exception("Solana Signature verification error")
            result = False

        return result
