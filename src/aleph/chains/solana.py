import json
import base58
from aleph.chains.common import get_verification_buffer
from aleph.register_chain import register_verifier

from nacl.signing import VerifyKey

import logging

LOGGER = logging.getLogger("chains.solana")
CHAIN_NAME = "SOL"


async def verify_signature(message):
    """Verifies a signature of a message, return True if verified, false if not"""

    try:
        signature = json.loads(message["signature"])
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

    if message["sender"] != signature["publicKey"]:
        LOGGER.exception("Solana signature source error")
        return False

    try:
        verify_key = VerifyKey(public_key)
        verification_buffer = await get_verification_buffer(message)
        verif = verify_key.verify(verification_buffer, signature=sigdata)
        result = verif == verification_buffer
    except Exception:
        LOGGER.exception("Solana Signature verification error")
        result = False

    return result


register_verifier(CHAIN_NAME, verify_signature)
