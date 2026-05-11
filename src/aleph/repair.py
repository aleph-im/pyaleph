import logging

from aleph_message.models import ItemHash
from sqlalchemy import select

from aleph.db.accessors.files import upsert_file
from aleph.db.models import StoredFileDb
from aleph.storage import StorageService
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.utils import get_sha256

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


def _is_storage_hash(h: str) -> bool:
    return len(h) == 64 and all(c in "0123456789abcdef" for c in h)


async def _fix_corrupt_storage_cache(
    session: DbSession, storage_service: StorageService
) -> int:
    """Delete locally cached storage files whose content does not match their SHA-256 hash.

    Only storage-type hashes (64-char hex) are checked. IPFS hashes are skipped —
    computing an IPFS hash requires a daemon round-trip. Corrupt entries are removed
    so they are refetched from the network on next access. Returns the count removed.
    """
    all_hashes = [
        f.hash
        for f in session.execute(select(StoredFileDb)).scalars().all()
        if _is_storage_hash(f.hash)
    ]

    LOGGER.info(
        "Checking %d storage-type cache entries for SHA-256 integrity", len(all_hashes)
    )

    removed = 0
    for file_hash in all_hashes:
        content = await storage_service.storage_engine.read(filename=file_hash)
        if content is None:
            continue
        if get_sha256(content) != file_hash:
            LOGGER.warning(
                "Corrupt cache entry '%s': SHA-256 mismatch, deleting", file_hash
            )
            await storage_service.storage_engine.delete(filename=file_hash)
            removed += 1

    LOGGER.info("Removed %d corrupt cache entries", removed)
    return removed


async def repair_node(
    storage_service: StorageService,
    session_factory: DbSessionFactory,
    repair_storage: bool = False,
):
    LOGGER.info("Fixing file sizes")
    with session_factory() as session:
        await _fix_file_sizes(session, storage_service, store_files=True)
        if repair_storage:
            await _fix_corrupt_storage_cache(session, storage_service)
        session.commit()
