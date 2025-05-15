import asyncio
import datetime as dt
import logging

from aioipfs import NotPinnedError
from aleph_message.models import ItemHash, ItemType
from configmanager import Config

from aleph.db.accessors.files import delete_file as delete_file_db
from aleph.db.accessors.files import delete_grace_period_file_pins, get_unpinned_files
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


class GarbageCollector:
    def __init__(
        self, session_factory: DbSessionFactory, storage_service: StorageService
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def _delete_from_ipfs(self, file_hash: ItemHash):
        ipfs_client = self.storage_service.ipfs_service.ipfs_client
        try:
            await ipfs_client.pin.rm(file_hash)
        except NotPinnedError:
            LOGGER.warning("File not pinned: %s", file_hash)
        except Exception as err:
            LOGGER.warning("Failed to unpin file %s: %s", file_hash, str(err))

        # Smaller IPFS files are cached in local storage
        LOGGER.debug("Deleting %s from local storage", file_hash)
        await self._delete_from_local_storage(file_hash)

        LOGGER.debug("Removed from IPFS: %s", file_hash)

    async def _delete_from_local_storage(self, file_hash: ItemHash):
        LOGGER.debug(f"Removing from local storage: {file_hash}")
        await self.storage_service.storage_engine.delete(file_hash)
        LOGGER.debug(f"Removed from local storage: {file_hash}")

    async def collect(self, datetime: dt.datetime):
        with self.session_factory() as session:
            # Delete outdated grace period file pins
            delete_grace_period_file_pins(session=session, datetime=datetime)
            session.commit()

            # Delete files without pins
            files_to_delete = list(get_unpinned_files(session))
            LOGGER.info("Found %d files to delete", len(files_to_delete))

            for file_to_delete in files_to_delete:
                try:
                    file_hash = ItemHash(file_to_delete.hash)
                    LOGGER.info("Deleting %s...", file_hash)

                    delete_file_db(session=session, file_hash=file_hash)
                    session.commit()

                    if file_hash.item_type == ItemType.ipfs:
                        await self._delete_from_ipfs(file_hash)
                    elif file_hash.item_type == ItemType.storage:
                        await self._delete_from_local_storage(file_hash)

                    LOGGER.info("Deleted %s", file_hash)
                except Exception as err:
                    LOGGER.error("Failed to delete file %s: %s", file_hash, str(err))


async def garbage_collector_task(
    config: Config, garbage_collector: GarbageCollector
) -> None:
    collection_interval = dt.timedelta(
        hours=config.storage.garbage_collector_period.value
    )

    while True:
        try:
            # Start by waiting, this gives the node time to start up and process potential pending
            # messages that could pin files.
            LOGGER.info(
                "Next garbage collector run: %s.", utc_now() + collection_interval
            )
            await asyncio.sleep(collection_interval.total_seconds())

            LOGGER.info("Starting garbage collection...")
            await garbage_collector.collect(datetime=utc_now())
            LOGGER.info("Garbage collector ran successfully.")
        except Exception as err:
            LOGGER.exception(
                "An unexpected error occurred during garbage collection.", exc_info=err
            )
