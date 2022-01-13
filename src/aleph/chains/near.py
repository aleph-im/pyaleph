import json
import base58
from aleph.chains.common import get_verification_buffer
from aleph.chains.register import register_verifier

from nacl.signing import VerifyKey

import logging

LOGGER = logging.getLogger("chains.near")
CHAIN_NAME = "NEAR"


async def verify_signature(message):
    """Verifies a signature of a message, return True if verified, false if not"""

    # Unpack the signature data
    try:
        sign_content = json.loads(message["signature"])
        s_signature = base58.b58decode(sign_content["signature"])
        s_public_key = base58.b58decode(sign_content["publicKey"])
    except Exception:
        LOGGER.exception("NEAR signature deserialization error")
        return False


    # Verify the authenticity of the signature
    try:
        verify_key = VerifyKey(s_public_key)
        verification_buffer = await get_verification_buffer(message)

        verified = verify_key.verify(verification_buffer, signature=s_signature)
        result = verified == verification_buffer
    except Exception:
        LOGGER.exception("NEAR Signature verification error")
        result = False

    return result


register_verifier(CHAIN_NAME, verify_signature)
