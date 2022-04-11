#!/usr/bin/env python3
"""
Checks and updates the configuration and key files of an Aleph node.

This script imports the migration scripts in the scripts/ directory one by one
and runs the `upgrade` function they must all implement. Alternatively,
it can revert operations using the `downgrade` function, if implemented.

Migration scripts must implement a check to verify if they need to run
as the tool itself has no way to determine which scripts have already
been executed in the past.
"""

import argparse
import asyncio
import importlib.util
import logging
import os
import sys
from types import ModuleType

from configmanager import Config

from aleph.config import get_defaults
from aleph.model import init_db_globals

LOGGER = logging.getLogger()

SERIALIZED_KEY_FILE = "serialized-node-secret.key"


def cli_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Checks and updates the configuration and key files of an Aleph node."
    )
    parser.add_argument(
        "command",
        action="store",
        choices=("upgrade", "downgrade"),
        type=str,
        help="Path to setup.cfg.",
    )
    parser.add_argument(
        "--config",
        action="store",
        required=True,
        type=str,
        help="Path to the Core Channel Node configuration file.",
    )
    parser.add_argument(
        "--key-dir",
        action="store",
        required=True,
        type=str,
        help="Path to the key directory.",
    )
    parser.add_argument(
        "--key-file",
        action="store",
        required=False,
        type=str,
        help="Path to the private key file, if any. Only used to upgrade the key to the latest format.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Show more information.",
        action="store_true",
        default=False,
    )
    return parser.parse_args()


def setup_logging(log_level: int) -> None:
    logformat = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=log_level,
        stream=sys.stdout,
        format=logformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def init_config(config_file: str) -> Config:
    config = Config(schema=get_defaults())
    config.yaml.load(config_file)
    return config


def import_module_from_path(path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location("migration_module", path)
    migration_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_module)
    return migration_module


async def main(args: argparse.Namespace):
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level)

    # Initialize some basic config and global variables
    config = init_config(args.config)
    init_db_globals(config=config)

    migration_scripts_dir = os.path.join(os.path.dirname(__file__), "scripts")
    migration_scripts = sorted(
        f for f in os.listdir(migration_scripts_dir) if f.endswith(".py")
    )

    command = args.command

    for migration_script in migration_scripts:
        migration_script_path = os.path.join(migration_scripts_dir, migration_script)
        migration_module = import_module_from_path(migration_script_path)

        if args.verbose:
            LOGGER.info(f"%s: %s", migration_script, migration_module.__doc__)
        LOGGER.info(f"Running %s for %s...", command, migration_script)
        migration_func = getattr(migration_module, args.command)

        kwargs = {
            "config_file": args.config,
            "key_dir": args.key_dir,
            "key_file": args.key_file,
            "config": config,
        }

        if asyncio.iscoroutinefunction(migration_func):
            await migration_func(**kwargs)
        else:
            migration_func(**kwargs)

    LOGGER.info(
        f"Successfully ran %s. You can now start the Core Channel Node.", command
    )


if __name__ == "__main__":
    try:
        asyncio.run(main(cli_parse()))
    except Exception as e:
        LOGGER.error("%s", str(e))
        sys.exit(1)
