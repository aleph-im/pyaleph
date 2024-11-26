import functools
import logging

from eth_account import Account
from eth_account.messages import encode_defunct

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.utils import run_in_executor

from .abc import Verifier

LOGGER = logging.getLogger("chains.evm")


class EVMVerifier(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        verification = get_verification_buffer(message)

        message_hash = await run_in_executor(
            None, functools.partial(encode_defunct, text=verification.decode("utf-8"))
        )

        verified = False
        try:
            # we assume the signature is a valid string
            address = await run_in_executor(
                None,
                functools.partial(
                    Account.recover_message, message_hash, signature=message.signature
                ),
            )
            if address == message.sender:
                verified = True
            else:
                LOGGER.warning(
                    "Received bad signature from %s for %s" % (address, message.sender)
                )
                return False

        except Exception:
            LOGGER.exception("Error processing signature for %s" % message.sender)
            verified = False

        return verified
