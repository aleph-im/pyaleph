import hashlib
import base58
import bech32
import struct
from coincurve.keys import PublicKey
from aleph.chains.common import get_verification_buffer
from .connector import Verifier

import logging

from aleph.schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger("chains.avalanche")
CHAIN_NAME = "AVAX"
MESSAGE_TEMPLATE = b"\x1AAvalanche Signed Message:\n%b"


async def pack_message(message):
    message = struct.pack(">I", len(message)) + message
    message = MESSAGE_TEMPLATE % message
    return message


async def validate_checksum(value):
    return value[:-4], hashlib.sha256(value[:-4]).digest()[-4:] == value[-4:]


async def address_from_public_key(pubk):
    if len(pubk) == 65:
        raise NotImplementedError("Can't handle this key yet")

    if len(pubk) == 33:
        shahash = hashlib.sha256(pubk).digest()
        ripehash = hashlib.new("rmd160")
        ripehash.update(shahash)
        ripehash = ripehash.digest()
        return ripehash

    raise ValueError("Impossible to hash this pubkey")


async def address_to_string(chain_id, hrp, address):
    bits = bech32.convertbits([x for x in address], 8, 5)
    encoded = bech32.bech32_encode("avax", bits)
    return f"{chain_id}-{encoded}"


async def get_chain_info(address):
    chain_id, rest = address.split("-", 1)
    hrp, rest = rest.split("1", 1)
    return chain_id, hrp


class AvalancheConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""
        try:
            chain_id, hrp = await get_chain_info(message.sender)
        except ValueError as e:
            LOGGER.warning("Avalanche sender address deserialization error: %s", str(e))
            return False

        try:
            signature = base58.b58decode(message.signature)
            signature, status = await validate_checksum(signature)
            if not status:
                LOGGER.exception("Avalanche signature checksum error")
                return False
        except Exception:
            LOGGER.exception("Avalanche signature deserialization error")
            return False

        try:
            verification = get_verification_buffer(message)
            verification = await pack_message(verification)

            public_key = PublicKey.from_signature_and_message(signature, verification)

            address = await address_from_public_key(public_key.format())
            address = await address_to_string(chain_id, hrp, address)

            result = address == message.sender

        except Exception as e:
            LOGGER.exception("Error processing signature for %s" % message.sender)
            result = False

        return result
