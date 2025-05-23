import pytest
import pytest_asyncio
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.messages import get_message_status
from aleph.db.models.files import MessageFilePinDb, StoredFileDb
from aleph.db.models.messages import MessageDb, MessageStatusDb
from aleph.services.storage.garbage_collector import GarbageCollector
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus


@pytest.fixture
def gc(
    session_factory: DbSessionFactory, test_storage_service: StorageService
) -> GarbageCollector:
    return GarbageCollector(
        session_factory=session_factory, storage_service=test_storage_service
    )


@pytest_asyncio.fixture
async def fixture_removing_messages(session_factory: DbSessionFactory):
    # Set up test data with messages in REMOVING status
    now = utc_now()

    # Create test data
    store_message_hash = "abcd" * 16
    store_message_file_hash = "1234" * 16

    # Message with REMOVING status that should be changed to REMOVED (no pinned files)
    store_message = MessageDb(
        item_hash=store_message_hash,
        sender="0xsender1",
        chain=Chain.ETH,
        type=MessageType.store,
        time=now,
        item_type=ItemType.ipfs,
        signature="sig1",
        size=1000,
        content={
            "type": "TEST",
            "item_hash": store_message_file_hash,
            "item_type": ItemType.ipfs.value,
        },
    )

    # Create file reference
    store_file = StoredFileDb(
        hash=store_message_file_hash,
        size=1000,
        type=FileType.FILE,
        # No pins - file is no longer pinned
    )

    # Message status with REMOVING
    store_message_status = MessageStatusDb(
        item_hash=store_message_hash,
        status=MessageStatus.REMOVING,
        reception_time=now,
    )

    # Message that should stay in REMOVING status (file still pinned)
    pinned_message_hash = "efgh" * 16
    pinned_file_hash = "5678" * 16

    pinned_message = MessageDb(
        item_hash=pinned_message_hash,
        sender="0xsender2",
        chain=Chain.ETH,
        type=MessageType.store,
        time=now,
        item_type=ItemType.ipfs,
        signature="sig2",
        size=1000,
        content={
            "type": "TEST",
            "item_hash": pinned_file_hash,
            "item_type": ItemType.ipfs.value,
        },
    )

    # Create file with pins
    pinned_file = StoredFileDb(
        hash=pinned_file_hash,
        size=2000,
        type=FileType.FILE,
        pins=[
            MessageFilePinDb(
                created=now,
                owner="0xowner1",
                item_hash="other_message_hash",
            )
        ],
    )

    # Message status with REMOVING
    pinned_message_status = MessageStatusDb(
        item_hash=pinned_message_hash,
        status=MessageStatus.REMOVING,
        reception_time=now,
    )

    with session_factory() as session:
        session.add_all(
            [
                store_message,
                store_file,
                store_message_status,
                pinned_message,
                pinned_file,
                pinned_message_status,
            ]
        )
        session.commit()

        yield {
            "removable_message": store_message_hash,
            "pinned_message": pinned_message_hash,
        }


@pytest.mark.asyncio
async def test_check_and_update_removing_messages(
    session_factory: DbSessionFactory, gc: GarbageCollector, fixture_removing_messages
):
    # Run the function that checks and updates message status
    await gc._check_and_update_removing_messages()

    with session_factory() as session:
        # The message with no pinned files should now have REMOVED status
        removable_status = get_message_status(
            session=session, item_hash=fixture_removing_messages["removable_message"]
        )
        assert removable_status is not None
        assert removable_status.status == MessageStatus.REMOVED

        # The message with a pinned file should still have REMOVING status
        pinned_status = get_message_status(
            session=session, item_hash=fixture_removing_messages["pinned_message"]
        )
        assert pinned_status is not None
        assert pinned_status.status == MessageStatus.REMOVING
