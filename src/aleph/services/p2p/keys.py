from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
    generate_private_key,
)
from cryptography.hazmat.backends import default_backend as crypto_default_backend

from typing import Optional, Tuple


def create_new_key_pair(
    key_size: int = 2048, exponent: int = 65537
) -> Tuple[RSAPrivateKey, RSAPublicKey]:
    private_key = generate_private_key(
        backend=crypto_default_backend(), public_exponent=exponent, key_size=key_size
    )

    public_key = private_key.public_key()

    return private_key, public_key


def save_private_key(private_key: RSAPrivateKey, file_path: str) -> None:
    with open(file_path, "wb") as f:
        f.write(
            private_key.private_bytes(
                crypto_serialization.Encoding.PEM,
                crypto_serialization.PrivateFormat.PKCS8,
                crypto_serialization.NoEncryption(),
            )
        )


def generate_keypair(
    print_key: bool, key_path: Optional[str]
) -> Tuple[RSAPrivateKey, RSAPublicKey]:
    """Generate a key pair and exit."""
    private_key, public_key = create_new_key_pair()

    if print_key or key_path:
        private_key_str = private_key.private_bytes(
            crypto_serialization.Encoding.PEM,
            crypto_serialization.PrivateFormat.PKCS8,
            crypto_serialization.NoEncryption(),
        ).decode("UTF-8")

        if print_key:
            # Print the armored key pair for archiving
            print(private_key_str)

        if key_path:
            # Save the armored key pair in a file
            with open(key_path, "w") as f:
                f.write(private_key_str)

    return private_key, public_key
