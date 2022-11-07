import json
import base58
from aleph.chains.common import get_verification_buffer
from aleph.register_chain import register_verifier

from nacl.signing import VerifyKey

import logging

from aleph.schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger("chains.solana")
CHAIN_NAME = "SOL"


async def verify_signature(message: BasePendingMessage) -> bool:
    """Verifies a signature of a message, return True if verified, false if not"""

    if message.signature is None:
        LOGGER.warning("'%s': missing signature.", message.item_hash)
        return False

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
        # verif = (get_verification_buffer(message)).decode('utf-8')
        # result = keypair.verify(verif, signature['data'])
    except Exception:
        LOGGER.exception("Solana Signature verification error")
        result = False

    return result


register_verifier(CHAIN_NAME, verify_signature)
