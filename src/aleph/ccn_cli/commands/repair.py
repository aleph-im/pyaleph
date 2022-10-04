"""
Repairs the local CCN by checking the following
"""
import asyncio
import itertools
from typing import Dict, FrozenSet
from typing import cast

import typer
from configmanager import Config

import aleph.model
from aleph.ccn_cli.cli_config import CliConfig
from aleph.config import get_defaults
from aleph.model import init_db_globals
from .toolkit.local_storage import list_expected_local_files

repair_ns = typer.Typer()



def print_files_to_preserve(files_to_preserve: Dict[str, FrozenSet[str]]) -> None:
    typer.echo("The following files will be preserved:")
    for file_type, files in files_to_preserve.items():
        typer.echo(f"* {len(files)} {file_type}")


async def list_missing_files() -> FrozenSet[str]:
    # Get a set of all the files currently in GridFS
    gridfs_files_dict = {
        file["filename"]: file
        async for file in aleph.model.db["fs.files"].find(
            projection={"_id": 0, "filename": 1, "length": 1, "uploadDate": 1},
            batch_size=1000,
        )
    }

    gridfs_files = frozenset(gridfs_files_dict.keys())
    typer.echo(f"Found {len(gridfs_files_dict)} files in local storage.")

    expected_local_files_dict = await list_expected_local_files()
    expected_local_files = frozenset(itertools.chain.from_iterable(expected_local_files_dict.values()))

    missing_files = expected_local_files - gridfs_files
    return missing_files


async def fetch_missing_files():
    missing_files = await list_missing_files()
    typer.echo(f"Found {len(missing_files)} missing files.")


async def run(ctx: typer.Context):
    config = Config(schema=get_defaults())
    cli_config = cast(CliConfig, ctx.obj)
    config.yaml.load(str(cli_config.config_file_path))

    init_db_globals(config=config)
    if aleph.model.db is None:  # for mypy
        raise ValueError("DB not initialized as expected.")

    await fetch_missing_files()

    typer.echo("Done.")


@repair_ns.command(name="run")
def run_repair(ctx: typer.Context):
    asyncio.run(run(ctx))
