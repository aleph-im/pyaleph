"""fix ipfs files size

Revision ID: 77e68941d36c
Revises: 039c56d3b33e
Create Date: 2023-04-25 10:53:44.111572

"""
import asyncio
import logging
from threading import Thread

import aioipfs
from alembic import op
from sqlalchemy import select, update

from aleph.config import get_config
from aleph.db.models import StoredFileDb
from aleph.services.ipfs.common import make_ipfs_client
from aleph.types.files import FileType

# revision identifiers, used by Alembic.
revision = "77e68941d36c"
down_revision = "039c56d3b33e"
branch_labels = None
depends_on = None


logger = logging.getLogger("alembic")


async def stat_ipfs(ipfs_client: aioipfs.AsyncIPFS, cid: str):
    try:
        return await asyncio.wait_for(ipfs_client.files.stat(f"/ipfs/{cid}"), 5)
    except TimeoutError:
        return None


async def upgrade_async() -> None:
    conn = op.get_bind()
    files = conn.execute(
        select(StoredFileDb.hash).where(
            (StoredFileDb.hash.like("Qm%") | StoredFileDb.hash.like("bafy%"))
            & (StoredFileDb.type == FileType.FILE)
        )
    ).all()

    config = get_config()
    ipfs_client = make_ipfs_client(host=config.ipfs.host.value, port=config.ipfs.port.value)

    for file in files:
        stats = await stat_ipfs(ipfs_client, cid=file.hash)
        if stats is None:
            logger.warning("Could not stat file: %s", file.hash)

        op.execute(
            update(StoredFileDb)
            .where(StoredFileDb.hash == file.hash)
            .values(size=stats["Size"])
        )

    await ipfs_client.close()


def upgrade_thread():
    asyncio.run(upgrade_async())


def upgrade() -> None:
    # We can reach this point from sync and async code, resulting in errors if an event loop
    # is already running if we just try to run the upgrade_async coroutine. The easiest
    # solution here is to start another thread and run the migration from there.
    thread = Thread(target=upgrade_thread, daemon=True)
    thread.start()
    thread.join()


def downgrade() -> None:
    # Don't reset sizes, it's pointless.
    pass
