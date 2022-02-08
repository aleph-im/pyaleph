"""
This migration moves the private key file to the key directory and generates two new keys:
a public key, and a serialized version of the private key for use by the P2P service.
"""


import logging
import os

from Crypto.PublicKey import RSA
from p2pclient.libp2p_stubs.crypto.rsa import KeyPair, RSAPrivateKey

from aleph.exceptions import InvalidKeyDirException
from aleph.services.keys import save_keys
from pathlib import Path

LOGGER = logging.getLogger(os.path.basename(__file__))


SERIALIZED_KEY_FILE = "serialized-node-secret.key"


def populate_key_dir(key_file: Path, output_dir: Path) -> None:
    with key_file.open() as f:
        private_key_str = f.read()

    private_key = RSAPrivateKey(RSA.import_key(private_key_str))
    key_pair = KeyPair(private_key=private_key, public_key=private_key.get_public_key())
    save_keys(key_pair, str(output_dir))


def upgrade(**kwargs):
    key_dir = Path(kwargs["key_dir"])
    key_file = Path(kwargs["key_file"])

    # Nothing to do if the serialized key file already exists
    serialized_key_file = key_dir / SERIALIZED_KEY_FILE
    if serialized_key_file.is_file():
        LOGGER.info(
            "Serialized key file {%s} already exists, nothing to do",
            serialized_key_file,
        )
        return

    if not key_dir.is_dir():
        raise InvalidKeyDirException(
            f"The specified key directory ('{key_dir}') is not a directory."
        )

    if key_file is None:
        raise ValueError("Key file path not specified.")

    LOGGER.info(
        "Migrating the private key in %s and using it to generate a public key "
        "and a serialized private key...",
        key_dir,
    )
    populate_key_dir(key_file, key_dir)
    LOGGER.info("Migrated the private/public keys in %s.", key_dir)


def downgrade(**kwargs):
    # Nothing to do, the key file is still present in the key directory
    pass
