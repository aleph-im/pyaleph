import logging

from aleph_message.models import ItemHash
from sqlalchemy import select

from aleph.db.accessors.files import upsert_file
from aleph.db.models import StoredFileDb
from aleph.storage import StorageService
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)


async def _fix_file_sizes(
    session: DbSession, storage_service: StorageService, store_files: bool
):
    files_with_negative_size = (
        session.execute(select(StoredFileDb).where(StoredFileDb.size < 0))
        .scalars()
        .all()
    )

    LOGGER.info("Found %d files with negative size", len(files_with_negative_size))

    for file in files_with_negative_size:
        file_hash = ItemHash(file.hash)
        LOGGER.info("Fixing file %s", file_hash)

        try:
            file_content = await storage_service.get_hash_content(
                content_hash=file_hash,
                engine=file_hash.item_type,
                use_network=True,
                use_ipfs=True,
                store_value=store_files,
            )
        except Exception:
            LOGGER.exception("Failed to fetch file %s", file_hash)
            continue

        upsert_file(
            session=session,
            file_hash=file_hash,
            file_type=file.type,
            size=len(file_content),
        )


async def repair_node(
    storage_service: StorageService, session_factory: DbSessionFactory
):
    LOGGER.info("Fixing file sizes")
    with session_factory() as session:
        await _fix_file_sizes(session, storage_service, store_files=True)
        session.commit()
