import functools
import logging
from typing import Optional

from eth_abi.abi import encode
from eth_account import Account
from eth_account.messages import _hash_eip191_message, encode_defunct
from eth_utils.address import to_checksum_address
from web3 import AsyncHTTPProvider, AsyncWeb3

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.utils import run_in_executor

from .abc import Verifier

LOGGER = logging.getLogger("chains.evm")

ERC6492_MAGIC = bytes.fromhex(
    "6492649264926492649264926492649264926492649264926492649264926492"
)
ERC1271_MAGIC = bytes.fromhex("1626ba7e")
IS_VALID_SIGNATURE_SELECTOR = bytes.fromhex("1626ba7e")
# isValidSigWithSideEffects(address,bytes32,bytes)
UNIVERSAL_VALIDATOR_SELECTOR = bytes.fromhex("8dca4bea")
# ERC-6492 UniversalSigValidator (deterministic CREATE2 address)
# See: https://eips.ethereum.org/EIPS/eip-6492
UNIVERSAL_VALIDATOR_ADDRESS = "0x0000000000002fd5Aeb385D324B580FCa7c83823"


class EVMVerifier(Verifier):
    def __init__(self, rpc_url: Optional[str] = None):
        self.rpc_url = rpc_url
        self._w3: Optional[AsyncWeb3] = None

    def _get_web3_client(self) -> Optional[AsyncWeb3]:
        if self.rpc_url is None:
            return None
        if self._w3 is None:
            self._w3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        return self._w3

    @staticmethod
    def _is_erc6492(sig_bytes: bytes) -> bool:
        return len(sig_bytes) >= 32 and sig_bytes[-32:] == ERC6492_MAGIC

    async def _verify_erc6492(
        self,
        w3: AsyncWeb3,
        sender: str,
        message_hash: bytes,
        signature: bytes,
    ) -> bool:
        """Validate an ERC-6492 counterfactual signature via UniversalSigValidator."""
        try:
            calldata = UNIVERSAL_VALIDATOR_SELECTOR + encode(
                ["address", "bytes32", "bytes"],
                [sender, message_hash, signature],
            )
            result = await w3.eth.call(
                {"to": UNIVERSAL_VALIDATOR_ADDRESS, "data": calldata}
            )
            return bool(int.from_bytes(result, "big"))
        except Exception:
            LOGGER.exception("Error calling UniversalSigValidator for %s", sender)
            return False

    async def _verify_erc1271(
        self,
        w3: AsyncWeb3,
        sender: str,
        message_hash: bytes,
        signature: bytes,
    ) -> bool:
        """Call isValidSignature on a deployed ERC-1271 contract."""
        try:
            calldata = IS_VALID_SIGNATURE_SELECTOR + encode(
                ["bytes32", "bytes"], [message_hash, signature]
            )
            result = await w3.eth.call({"to": sender, "data": calldata})
            return result[:4] == ERC1271_MAGIC
        except Exception:
            LOGGER.exception("Error calling isValidSignature on %s", sender)
            return False

    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not.

        Detection / fallback order (cheapest first, except ERC-6492 which is detected upfront):
          - ERC-6492 magic suffix → UniversalSigValidator eth_call (skip ECDSA)
          - Plain ECDSA ecrecover (0 RPC calls)
          - ERC-1271 isValidSignature on deployed contract (1 eth_call)
        """
        verification = get_verification_buffer(message)
        message_hash_obj = await run_in_executor(
            None,
            functools.partial(encode_defunct, text=verification.decode("utf-8")),
        )
        raw_hash: bytes = _hash_eip191_message(message_hash_obj)

        if not message.signature:
            return False

        try:
            sig_bytes = bytes.fromhex(message.signature.removeprefix("0x"))
        except ValueError:
            sig_bytes = b""

        sender_checksum = to_checksum_address(message.sender)

        # Path 1: ERC-6492 counterfactual (magic suffix detected, skip ECDSA)
        if self._is_erc6492(sig_bytes):
            w3 = self._get_web3_client()
            if w3 is None:
                LOGGER.warning(
                    "ERC-6492 signature for %s but no rpc_url configured",
                    message.sender,
                )
                return False
            return await self._verify_erc6492(w3, sender_checksum, raw_hash, sig_bytes)

        # Path 2: plain ECDSA
        try:
            address = await run_in_executor(
                None,
                functools.partial(
                    Account.recover_message,
                    message_hash_obj,
                    signature=message.signature,
                ),
            )
            if address.lower() == message.sender.lower():
                return True
            LOGGER.warning(
                "ECDSA recovered %s != sender %s, falling back to ERC-1271",
                address,
                message.sender,
            )
        except Exception:
            LOGGER.debug(
                "ECDSA recovery failed for %s, trying ERC-1271", message.sender
            )

        # Path 3: ERC-1271 (deployed contract)
        w3 = self._get_web3_client()
        if w3 is None:
            LOGGER.warning(
                "Signature for %s failed ECDSA and no rpc_url configured for ERC-1271",
                message.sender,
            )
            return False

        try:
            code = await w3.eth.get_code(sender_checksum)
        except Exception:
            LOGGER.exception("Error checking contract code for %s", message.sender)
            return False

        if not code or code == b"0x":
            return False

        return await self._verify_erc1271(w3, sender_checksum, raw_hash, sig_bytes)
