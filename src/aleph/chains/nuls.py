import logging
import struct

from aleph_client.chains.nuls1 import (
    NulsSignature,
    hash_from_address,
    public_key_to_hash,
    address_from_hash,
)

from aleph.chains.common import get_verification_buffer
from aleph.register_chain import register_verifier

from aleph.utils import run_in_executor

LOGGER = logging.getLogger("chains.nuls")
CHAIN_NAME = "NULS"


async def verify_signature(message):
    """Verifies a signature of a message, return True if verified, false if not"""
    sig_raw = bytes(bytearray.fromhex(message["signature"]))
    sig = NulsSignature(sig_raw)

    sender_hash = hash_from_address(message["sender"])
    (sender_chain_id,) = struct.unpack("h", sender_hash[:2])

    hash = public_key_to_hash(sig.pub_key, sender_chain_id)

    address = address_from_hash(hash)
    if address != message["sender"]:
        LOGGER.warning(
            "Received bad signature from %s for %s" % (address, message["sender"])
        )
        return False

    verification = await get_verification_buffer(message)
    try:
        result = await run_in_executor(None, sig.verify, verification)
    except Exception:
        LOGGER.exception("NULS Signature verification error")
        result = False
    return result


register_verifier(CHAIN_NAME, verify_signature)
