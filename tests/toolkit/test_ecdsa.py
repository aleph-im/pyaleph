import base64
import time
import pytest
from unittest.mock import patch

from aleph.toolkit.ecdsa import (
    generate_key_pair,
    generate_key_pair_from_private_key,
    sign_message,
    verify_signature,
    create_auth_token,
    verify_auth_token,
)


def test_generate_key_pair():
    """Test key pair generation."""
    private_key, public_key = generate_key_pair()

    # Keys should be hex strings
    assert isinstance(private_key, str)
    assert isinstance(public_key, str)

    # Private key should be 64 hex characters (32 bytes)
    assert len(private_key) == 64

    # Public key should be 66 hex characters (33 bytes compressed)
    assert len(public_key) == 66

    # Public key should start with 02 or 03 (compressed format)
    assert public_key.startswith(('02', '03'))


def test_generate_key_pair_from_private_key():
    """Test key pair generation from existing private key."""
    test_private_key = "646fa150ca94320b8eca3bd28d106703f3602dfb13b6b982cc17d17fd1182f85"

    private_key, public_key = generate_key_pair_from_private_key(test_private_key)

    # Should return the same private key
    assert private_key == test_private_key

    # Public key should be correctly derived
    assert isinstance(public_key, str)
    assert len(public_key) == 66
    assert public_key.startswith(('02', '03'))


def test_sign_and_verify_message():
    """Test message signing and verification."""
    private_key, public_key = generate_key_pair()
    message = "test message"

    # Sign the message
    signature = sign_message(message, private_key)

    # Signature should be base64 encoded
    assert isinstance(signature, str)
    # Should be valid base64
    base64.b64decode(signature)

    # Verify the signature
    is_valid = verify_signature(message, signature, public_key)
    assert is_valid is True

    # Verification should fail with wrong message
    is_valid_wrong = verify_signature("wrong message", signature, public_key)
    assert is_valid_wrong is False

    # Verification should fail with wrong public key
    _, wrong_public_key = generate_key_pair()
    is_valid_wrong_key = verify_signature(message, signature, wrong_public_key)
    assert is_valid_wrong_key is False


def test_create_and_verify_auth_token():
    """Test authentication token creation and verification."""
    private_key, public_key = generate_key_pair()

    # Create a token
    token = create_auth_token(private_key)

    # Token should be base64 encoded
    assert isinstance(token, str)
    # Should be valid base64
    token_data = base64.b64decode(token).decode("utf-8")

    # Token should contain timestamp and signature separated by colon
    parts = token_data.split(":", 1)
    assert len(parts) == 2

    timestamp_str, signature = parts
    # Timestamp should be a valid integer
    timestamp = int(timestamp_str)
    assert timestamp > 0

    # Signature should be valid base64
    base64.b64decode(signature)

    # Verify the token
    is_valid = verify_auth_token(token, public_key)
    assert is_valid is True


def test_verify_auth_token_with_wrong_public_key():
    """Test token verification fails with wrong public key."""
    private_key, _ = generate_key_pair()
    _, wrong_public_key = generate_key_pair()

    token = create_auth_token(private_key)

    # Should fail with wrong public key
    is_valid = verify_auth_token(token, wrong_public_key)
    assert is_valid is False


def test_verify_auth_token_expired():
    """Test token verification fails when token is expired."""
    private_key, public_key = generate_key_pair()

    # Mock time to create an old token
    old_timestamp = int(time.time()) - 600  # 10 minutes ago

    with patch('aleph.toolkit.ecdsa.time.time', return_value=old_timestamp):
        token = create_auth_token(private_key)

    # Should fail with default max_age (5 minutes)
    is_valid = verify_auth_token(token, public_key)
    assert is_valid is False

    # Should pass with larger max_age
    is_valid_long = verify_auth_token(token, public_key, max_age_seconds=700)
    assert is_valid_long is True


def test_verify_auth_token_future_timestamp():
    """Test token verification handles future timestamps within tolerance."""
    private_key, public_key = generate_key_pair()

    # Mock time to create a future token
    future_timestamp = int(time.time()) + 60  # 1 minute in future

    with patch('aleph.toolkit.ecdsa.time.time', return_value=future_timestamp):
        token = create_auth_token(private_key)

    # Should pass as it uses abs() for time difference
    is_valid = verify_auth_token(token, public_key)
    assert is_valid is True


def test_verify_auth_token_malformed():
    """Test token verification fails with malformed tokens."""
    _, public_key = generate_key_pair()

    # Invalid base64
    is_valid = verify_auth_token("invalid_base64!", public_key)
    assert is_valid is False

    # Valid base64 but wrong format (no colon)
    malformed_token = base64.b64encode("no_colon_here".encode()).decode()
    is_valid = verify_auth_token(malformed_token, public_key)
    assert is_valid is False

    # Valid base64 but invalid timestamp
    malformed_data = "not_a_number:MEQCIHFHKoBPyY3pCMY9x5gS4P1"
    malformed_token = base64.b64encode(malformed_data.encode()).decode()
    is_valid = verify_auth_token(malformed_token, public_key)
    assert is_valid is False


def test_verify_signature_with_invalid_inputs():
    """Test verify_signature handles invalid inputs gracefully."""
    _, public_key = generate_key_pair()

    # Invalid base64 signature
    is_valid = verify_signature("message", "invalid_base64!", public_key)
    assert is_valid is False

    # Invalid public key
    is_valid = verify_signature("message", "dGVzdA==", "invalid_public_key")
    assert is_valid is False


def test_token_roundtrip_with_known_values():
    """Test token creation and verification with known test values."""
    # Use known values for reproducible testing
    test_private_key = "50b44756efbcb9266d974af8a8bcecb97d960fd8ddaadd31ecf2082c757fcaad"
    test_public_key = "023d3b6f2e92e5d30b8d75291087051f6ef9425abbb626bebc3a5b358bce6007ee"

    # Create token
    token = create_auth_token(test_private_key)

    # Verify it works
    is_valid = verify_auth_token(token, test_public_key)
    assert is_valid is True

    # Verify with wrong key fails
    _, wrong_key = generate_key_pair()
    is_valid_wrong = verify_auth_token(token, wrong_key)
    assert is_valid_wrong is False