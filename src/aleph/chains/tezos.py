import json
import logging

from aleph_pytezos.crypto.key import Key

from aleph.chains.common import get_verification_buffer
from aleph.register_chain import register_verifier
from aleph.schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger(__name__)
CHAIN_NAME = "TEZOS"


async def verify_signature(message: BasePendingMessage) -> bool:
    """
    Verifies the cryptographic signature of a message signed with a Tezos key.
    """

    verification_buffer = get_verification_buffer(message)
    try:
        signature_dict = json.loads(message.signature)
    except json.JSONDecodeError:
        LOGGER.warning("Signature field for Tezos message is not JSON deserializable.")
        return False

    try:
        signature = signature_dict["signature"]
        public_key = signature_dict["publicKey"]
    except KeyError as e:
        LOGGER.exception("'%s' key missing from Tezos signature dictionary.", e.args[0])
        return False

    key = Key.from_encoded_key(public_key)
    # Check that the sender ID is equal to the public key hash
    public_key_hash = key.public_key_hash()

    if message.sender != public_key_hash:
        LOGGER.warning(
            "Sender ID (%s) does not match public key hash (%s)",
            message.sender,
            public_key_hash,
        )

    # Check the signature
    try:
        key.verify(signature, verification_buffer)
    except ValueError:
        LOGGER.warning("Received message with bad signature from %s" % message.sender)
        return False

    return True


register_verifier(CHAIN_NAME, verify_signature)
