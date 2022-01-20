import argparse
from p2pclient.libp2p_stubs.crypto.rsa import KeyPair, RSAPrivateKey
from aleph.services.keys import save_keys
from Crypto.PublicKey import RSA


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extracts dependencies from setup.cfg into a requirements.txt file."
    )
    parser.add_argument(
        "--key-file", "-k", action="store", required=True, type=str, help="Path to the key file of the node."
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        action="store",
        default="keys",
        type=str,
        help="Path to the directory where the new keys must be saved.",
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    key_file = args.key_file
    output_dir = args.output_dir

    with open(key_file) as f:
        private_key_str = f.read()

    private_key = RSAPrivateKey(RSA.import_key(private_key_str))
    key_pair = KeyPair(private_key=private_key, public_key=private_key.get_public_key())
    save_keys(key_pair, output_dir)


if __name__ == "__main__":
    main(cli_parse())
