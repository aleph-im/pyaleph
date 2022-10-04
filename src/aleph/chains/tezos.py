import datetime as dt
import json
import logging
from enum import Enum

from aleph_pytezos.crypto.key import Key
from nacl.exceptions import BadSignatureError

from aleph.chains.common import get_verification_buffer
from aleph.chains.connector import Verifier
from aleph.db.models import PendingMessageDb
from aleph.schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger(__name__)

# Default dApp URL for Micheline-style signatures
DEFAULT_DAPP_URL = "aleph.im"


class TezosSignatureType(str, Enum):
    RAW = "raw"
    MICHELINE = "micheline"


def datetime_to_iso_8601(datetime: dt.datetime) -> str:
    """
    Returns the timestamp formatted to ISO-8601, JS-style.

    Compared to the regular `isoformat()`, this function only provides precision down
    to milliseconds and prints a "Z" instead of +0000 for UTC.
    This format is typically used by JavaScript applications, like our TS SDK.

    Example: 2022-09-23T14:41:19.029Z

    :param datetime: The timestamp to format.
    :return: The formatted timestamp.
    """

    date_str = datetime.strftime("%Y-%m-%d")
    time_str = f"{datetime.hour:02d}:{datetime.minute:02d}:{datetime.second:02d}.{datetime.microsecond // 1000:03d}"
    return f"{date_str}T{time_str}Z"


def micheline_verification_buffer(
    verification_buffer: bytes,
    datetime: dt.datetime,
    dapp_url: str,
) -> bytes:
    """
    Computes the verification buffer for Micheline-type signatures.

    This verification buffer is used when signing data with a Tezos web wallet.
    See https://tezostaquito.io/docs/signing/#generating-a-signature-with-beacon-sdk.

    :param verification_buffer: The original (non-Tezos) verification buffer for the Aleph message.
    :param datetime: Timestamp of the message.
    :param dapp_url: The URL of the dApp, for use as part of the verification buffer.
    :return: The verification buffer used for the signature by the web wallet.
    """

    prefix = b"Tezos Signed Message:"
    timestamp = datetime_to_iso_8601(datetime).encode("utf-8")

    payload = b" ".join(
        (prefix, dapp_url.encode("utf-8"), timestamp, verification_buffer)
    )
    hex_encoded_payload = payload.hex()
    payload_size = str(len(hex_encoded_payload)).encode("utf-8")

    return b"\x05" + b"\x01\x00" + payload_size + payload


def get_tezos_verification_buffer(
    message: PendingMessageDb, signature_type: TezosSignatureType, dapp_url: str
) -> bytes:
    verification_buffer = get_verification_buffer(message)  # type: ignore

    if signature_type == TezosSignatureType.RAW:
        return verification_buffer
    elif signature_type == TezosSignatureType.MICHELINE:
        return micheline_verification_buffer(
            verification_buffer, message.time, dapp_url
        )

    raise ValueError(f"Unsupported signature type: {signature_type}")


class TezosConnector(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """
        Verifies the cryptographic signature of a message signed with a Tezos key.
        """

        try:
            signature_dict = json.loads(message.signature)
        except json.JSONDecodeError:
            LOGGER.warning(
                "Signature field for Tezos message is not JSON deserializable."
            )
            return False

        try:
            signature = signature_dict["signature"]
            public_key = signature_dict["publicKey"]
        except KeyError as e:
            LOGGER.exception(
                "'%s' key missing from Tezos signature dictionary.", e.args[0]
            )
            return False

        signature_type = TezosSignatureType(signature_dict.get("signingType", "raw"))
        dapp_url = signature_dict.get("dAppUrl", DEFAULT_DAPP_URL)

        key = Key.from_encoded_key(public_key)
        # Check that the sender ID is equal to the public key hash
        public_key_hash = key.public_key_hash()

        if message.sender != public_key_hash:
            LOGGER.warning(
                "Sender ID (%s) does not match public key hash (%s)",
                message.sender,
                public_key_hash,
            )

        verification_buffer = get_tezos_verification_buffer(
            message, signature_type, dapp_url   # type: ignore
        )

        # Check the signature
        try:
            key.verify(signature, verification_buffer)
        except (ValueError, BadSignatureError):
            LOGGER.warning(
                "Received message with bad signature from %s" % message.sender
            )
            return False

        return True
