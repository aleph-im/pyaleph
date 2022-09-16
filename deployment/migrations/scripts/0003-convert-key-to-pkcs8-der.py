"""
This migration converts the PEM private key file to PKCS8 DER for compatibility
with the new Aleph.im P2P service. The Rust implementation of libp2p can only load
RSA keys in that format.
"""


import logging
import os
from pathlib import Path
from typing import Optional

import yaml
from Crypto.PublicKey import RSA
from p2pclient.libp2p_stubs.crypto.rsa import RSAPrivateKey

from aleph.exceptions import InvalidKeyDirException

LOGGER = logging.getLogger(os.path.basename(__file__))


PKCS8_DER_KEY_FILE = "node-secret.pkcs8.der"


def convert_pem_key_file_to_pkcs8_der(
    pem_key_file: Path, pkcs8_der_key_file: Path
) -> None:
    with pem_key_file.open() as pem:
        private_key = RSAPrivateKey(RSA.import_key(pem.read()))

    with pkcs8_der_key_file.open("wb") as der:
        der.write(private_key.impl.export_key(format="DER", pkcs=8))


def get_key_from_config(config_file: Path) -> Optional[str]:
    """
    In previous versions of the CCN, it was possible to set the key value directly
    in the config file. This function tries to find it in the config or returns None.

    :param config_file: Path to the CCN configuration file.
    :return: The private key used to identify the node on the P2P network, or None
             if the key is not provided in the config file.
    """
    with open(config_file) as f:
        config = yaml.safe_load(f)

    try:
        return config["p2p"]["key"]
    except KeyError:
        return None


def upgrade(**kwargs):
    key_dir = Path(kwargs["key_dir"])
    pem_key_file = key_dir / "node-secret.key"

    # Nothing to do if the PKCS8 DER key file already exists
    pkcs8_der_key_file = key_dir / PKCS8_DER_KEY_FILE
    if pkcs8_der_key_file.is_file():
        LOGGER.info(
            "Key file %s already exists, nothing to do",
            pkcs8_der_key_file,
        )
        return

    if not key_dir.is_dir():
        raise InvalidKeyDirException(
            f"The specified key directory ('{key_dir}') is not a directory."
        )

    LOGGER.info("Converting the private key file to PKCS8 DER format...")
    convert_pem_key_file_to_pkcs8_der(pem_key_file, pkcs8_der_key_file)
    LOGGER.info("Successfully created %s.", pkcs8_der_key_file)


def downgrade(**kwargs):
    # Nothing to do, the key file is still present in the key directory
    pass
