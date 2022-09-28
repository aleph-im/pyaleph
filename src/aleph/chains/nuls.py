import logging
import struct

from aleph_client.chains.nuls1 import (
    NulsSignature,
    hash_from_address,
    public_key_to_hash,
    address_from_hash,
)

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.utils import run_in_executor
from .connector import Verifier

LOGGER = logging.getLogger("chains.nuls")
CHAIN_NAME = "NULS"


class NulsConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""
        sig_raw = bytes(bytearray.fromhex(message.signature))
        sig = NulsSignature(sig_raw)

        sender_hash = hash_from_address(message.sender)
        (sender_chain_id,) = struct.unpack("h", sender_hash[:2])

        hash = public_key_to_hash(sig.pub_key, sender_chain_id)

        address = address_from_hash(hash)
        if address != message.sender:
            LOGGER.warning(
                "Received bad signature from %s for %s" % (address, message.sender)
            )
            return False

        verification = get_verification_buffer(message)
        try:
            result = await run_in_executor(None, sig.verify, verification)
        except Exception:
            LOGGER.exception("NULS Signature verification error")
            result = False

        return result
