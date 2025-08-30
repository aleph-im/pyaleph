import base64
import time
from functools import wraps
from typing import Tuple

from aiohttp import web
from coincurve import PrivateKey, PublicKey

from aleph.config import get_config


def generate_key_pair() -> Tuple[str, str]:
    """Generate a new ECDSA key pair using secp256k1 curve.

    Returns:
        Tuple of (private_key_hex, public_key_hex)
    """
    private_key = PrivateKey()
    public_key = private_key.public_key

    return private_key.to_hex(), public_key.format(compressed=True).hex()


def generate_key_pair_from_private_key(private_key_hex: str) -> Tuple[str, str]:
    """Generate a key pair from an existing private key in hex format.

    Args:
        private_key_hex: Private key in hex format

    Returns:
        Tuple of (private_key_hex, public_key_hex)
    """
    private_key = PrivateKey.from_hex(private_key_hex)
    public_key = private_key.public_key

    return private_key.to_hex(), public_key.format(compressed=True).hex()


def sign_message(message: str, private_key_hex: str) -> str:
    """Sign a message with the private key and return base64 encoded signature.

    Args:
        message: The message to sign
        private_key_hex: Private key in hex format

    Returns:
        Base64 encoded signature
    """
    private_key = PrivateKey.from_hex(private_key_hex)
    message_bytes = message.encode("utf-8")
    signature = private_key.sign(message_bytes)
    return base64.b64encode(signature).decode("utf-8")


def verify_signature(message: str, signature_b64: str, public_key_hex: str) -> bool:
    """Verify a signature against a message and public key.

    Args:
        message: The original message
        signature_b64: Base64 encoded signature
        public_key_hex: Public key in hex format

    Returns:
        True if signature is valid, False otherwise
    """
    try:
        public_key = PublicKey.from_hex(public_key_hex)
        signature = base64.b64decode(signature_b64)
        message_bytes = message.encode("utf-8")
        return public_key.verify(signature, message_bytes)
    except Exception:
        return False


def create_auth_token(private_key_hex: str) -> str:
    """Create an authentication token with current timestamp.

    Args:
        private_key_hex: Private key in hex format

    Returns:
        Base64 encoded token containing timestamp and signature
    """
    timestamp = str(int(time.time()))
    signature = sign_message(timestamp, private_key_hex)
    token_data = f"{timestamp}:{signature}"
    return base64.b64encode(token_data.encode("utf-8")).decode("utf-8")


def verify_auth_token(
    token_b64: str, public_key_hex: str, max_age_seconds: int = 300
) -> bool:
    """Verify an authentication token.

    Args:
        token_b64: Base64 encoded token
        public_key_hex: Public key in hex format
        max_age_seconds: Maximum token age in seconds (default: 5 minutes)

    Returns:
        True if token is valid and not expired, False otherwise
    """
    try:
        token_data = base64.b64decode(token_b64).decode("utf-8")
        timestamp_str, signature = token_data.split(":", 1)

        # Check timestamp validity
        timestamp = int(timestamp_str)
        current_time = int(time.time())
        if abs(current_time - timestamp) > max_age_seconds:
            return False

        # Verify signature
        return verify_signature(timestamp_str, signature, public_key_hex)
    except Exception:
        return False


def require_auth_token(handler):
    """Decorator to require and verify authentication token from X-Auth-Token header."""

    @wraps(handler)
    async def wrapper(request: web.Request):
        # Get the auth token from headers
        auth_token = request.headers.get("X-Auth-Token")
        if not auth_token:
            raise web.HTTPUnauthorized(
                body="Missing X-Auth-Token header",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Get configuration
        config = get_config()
        auth_config = config.aleph.auth
        public_key = auth_config.public_key
        max_age = auth_config.max_token_age

        # Verify the token
        if not verify_auth_token(auth_token, public_key, max_age):
            raise web.HTTPUnauthorized(
                body="Invalid or expired authentication token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Token is valid, proceed with the handler
        return await handler(request)

    return wrapper
