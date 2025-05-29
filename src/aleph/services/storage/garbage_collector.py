import asyncio
import datetime as dt
import logging

from aioipfs import NotPinnedError
from aleph_message.models import ItemHash, ItemType, MessageType
from configmanager import Config

from aleph.db.accessors.cost import delete_costs_for_forgotten_and_deleted_messages
from aleph.db.accessors.files import delete_file as delete_file_db
from aleph.db.accessors.files import (
    delete_grace_period_file_pins,
    file_pin_exists,
    get_unpinned_files,
)
from aleph.db.accessors.messages import (
    get_matching_hashes,
    get_one_message_by_item_hash,
    make_message_status_upsert_query,
)
from aleph.db.models.messages import MessageStatusDb
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

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

    async def _check_and_update_removing_messages(self):
        """
        Check all messages with status REMOVING and update to REMOVED if their resources
        have been fully deleted.
        """
        LOGGER.info("Checking messages with REMOVING status")

        with self.session_factory() as session:
            # Get all messages with REMOVING status
            removing_messages = list(
                get_matching_hashes(
                    session=session,
                    status=MessageStatus.REMOVING,
                    hash_only=False,
                    pagination=0,  # Get all matching messages
                )
            )

            LOGGER.info(
                "Found %d messages with REMOVING status", len(removing_messages)
            )

            for message_status in removing_messages:
                item_hash = message_status.item_hash
                try:
                    # For STORE messages, check if the file is still pinned
                    # We need to get message details to check its type
                    message = get_one_message_by_item_hash(
                        session=session, item_hash=item_hash
                    )

                    resources_deleted = True

                    if message and message.type == MessageType.store:
                        # Check if the file is still pinned (by item_hash cause there could be other messages pinning the same file_hash)
                        if file_pin_exists(session=session, item_hash=item_hash):
                            resources_deleted = False

                    # If all resources have been deleted, update status to REMOVED
                    if resources_deleted:
                        now = utc_now()
                        session.execute(
                            make_message_status_upsert_query(
                                item_hash=item_hash,
                                new_status=MessageStatus.REMOVED,
                                reception_time=now,
                                where=(
                                    MessageStatusDb.status == MessageStatus.REMOVING
                                ),
                            )
                        )

                except Exception as err:
                    LOGGER.error(
                        "Failed to check or update message status %s: %s",
                        item_hash,
                        str(err),
                    )

            delete_costs_for_forgotten_and_deleted_messages(session=session)

            session.commit()

    async def collect(self, datetime: dt.datetime):
        with self.session_factory() as session:
            # Delete outdated grace period file pins
            delete_grace_period_file_pins(session=session, datetime=datetime)
            session.commit()

            # Delete files without pins
            files_to_delete = list(get_unpinned_files(session))
            LOGGER.info("Found %d files to delete", len(files_to_delete))

        for file_to_delete in files_to_delete:
            with self.session_factory() as session:
                try:
                    file_hash = ItemHash(file_to_delete.hash)
                    LOGGER.info("Deleting %s...", file_hash)

                    delete_file_db(session=session, file_hash=file_hash)

                    if file_hash.item_type == ItemType.ipfs:
                        await self._delete_from_ipfs(file_hash)
                    elif file_hash.item_type == ItemType.storage:
                        await self._delete_from_local_storage(file_hash)

                    session.commit()

                    LOGGER.info("Deleted %s", file_hash)
                except Exception as err:
                    LOGGER.error("Failed to delete file %s: %s", file_hash, str(err))
                    session.rollback()

        # After collecting garbage, check and update message status
        await self._check_and_update_removing_messages()


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
