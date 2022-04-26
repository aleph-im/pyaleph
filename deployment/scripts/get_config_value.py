"""
This script reads a configuration value from the CCN config. This enables reading
configuration values from shell scripts without launching the CCN itself.
"""

import argparse
import sys
from functools import partial

import configmanager.exceptions
from configmanager import Config

from aleph.config import get_defaults


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reads the specified CCN configuration key."
    )
    parser.add_argument(
        "--config-file",
        action="store",
        type=str,
        required=True,
        help="Path to the user configuration file.",
    )
    parser.add_argument(
        "config_key",
        action="store",
        type=str,
        help="Configuration key to retrieve.",
    )
    return parser.parse_args()


def load_config(config_file: str) -> Config:
    config = Config(schema=get_defaults())
    config.yaml.load(config_file)
    return config


print_err = partial(print, file=sys.stderr)


def main(args: argparse.Namespace):
    config_file = args.config_file
    config_key = args.config_key

    config = load_config(config_file)

    current_section = config
    sections = config_key.split(".")
    try:
        for section_name in sections:
            current_section = getattr(current_section, section_name)
        print(current_section.value)
    except configmanager.exceptions.NotFound:
        print_err(f"Configuration key not found: '{config_key}'.")
        sys.exit(-1)


if __name__ == "__main__":
    main(cli_parse())
