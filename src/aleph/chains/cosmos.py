import base64
import hashlib
import json
import logging
from typing import Dict

import ecdsa
from cosmospy import pubkey_to_address
from ecdsa import BadSignatureError

from aleph.chains.common import get_verification_buffer
from aleph.schemas.pending_messages import BasePendingMessage
from .connector import Verifier

LOGGER = logging.getLogger("chains.cosmos")
CHAIN_NAME = "CSDK"


async def get_signable_message(message: BasePendingMessage) -> Dict:
    signable = (get_verification_buffer(message)).decode("utf-8")
    content_message = {
        "type": "signutil/MsgSignText",
        "value": {
            "message": signable,
            "signer": message.sender,
        },
    }

    return {
        "chain_id": "signed-message-v1",
        "account_number": str(0),
        "fee": {
            "amount": [],
            "gas": str(0),
        },
        "memo": "",
        "sequence": str(0),
        "msgs": [
            content_message,
        ],
    }


async def get_verification_string(message: BasePendingMessage) -> str:
    value = await get_signable_message(message)
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


async def get_hrp(address):
    hrp, rest = address.split("1", 1)
    return hrp


class CosmosConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        if message.signature is None:
            LOGGER.warning("'%s': missing signature.", message.item_hash)
            return False

        try:
            if message.signature is None:
                LOGGER.warning("'%s': missing signature.", message.item_hash)
                return False

            signature = json.loads(message.signature)
        except Exception:
            LOGGER.exception("Cosmos signature deserialization error")
            return False

        try:
            if signature.get("pub_key").get("type") != "tendermint/PubKeySecp256k1":
                LOGGER.warning(
                    "Unsupported curve %s" % signature.get("pub_key").get("type")
                )
        except Exception:
            LOGGER.exception("Cosmos signature Key error")
            return False

        try:
            pub_key = base64.b64decode(signature.get("pub_key").get("value"))
            hrp = await get_hrp(message.sender)
        except Exception:
            LOGGER.exception("Cosmos key verification error")
            return False

        try:
            sig_compact = base64.b64decode(signature.get("signature"))
        except Exception:
            LOGGER.exception("Cosmos signature deserialization error")
            return False

        try:
            address = pubkey_to_address(pub_key, hrp=hrp)
            if address != message.sender:
                LOGGER.warning(
                    "Signature for bad address %s instead of %s"
                    % (address, message.sender)
                )
                return False

            verif = await get_verification_string(message)
            vk = ecdsa.VerifyingKey.from_string(pub_key, curve=ecdsa.SECP256k1)
            verified = vk.verify(
                sig_compact, verif.encode("utf-8"), hashfunc=hashlib.sha256
            )
            return verified

        except BadSignatureError:
            return False
        except Exception:
            LOGGER.exception("Substrate Signature verification error")
            return False
