import pytest
import pytest_asyncio
from aleph_message.models import Chain, ItemHash, ItemType, MessageType

from aleph.db.accessors.messages import get_message_status, get_removed_message
from aleph.db.models.files import FilePinType, MessageFilePinDb, StoredFileDb
from aleph.db.models.messages import MessageDb, MessageStatusDb, RemovedMessageDb
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
        status_value=MessageStatus.REMOVING,
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
        status_value=MessageStatus.REMOVING,
    )

    # Create file with pins
    pinned_file = StoredFileDb(
        hash=pinned_file_hash,
        size=2000,
        type=FileType.FILE,
    )

    # Create a separate pin for the file
    pinned_file_pin = MessageFilePinDb(
        item_hash=pinned_message_hash,
        file_hash=pinned_file_hash,
        type=FilePinType.MESSAGE,
        created=now,
        owner="0xowner1",
    )

    # Message status with REMOVING
    pinned_message_status = MessageStatusDb(
        item_hash=pinned_message_hash,
        status=MessageStatus.REMOVING,
        reception_time=now,
    )

    # Size snapshot taken when the removable message entered REMOVING
    store_removed_message = RemovedMessageDb(item_hash=store_message_hash, size=1000)

    # Removable message that entered REMOVING before the size snapshot
    # existed: no removed_messages row.
    legacy_message_hash = "ijkl" * 16
    legacy_message = MessageDb(
        item_hash=legacy_message_hash,
        sender="0xsender3",
        chain=Chain.ETH,
        type=MessageType.store,
        time=now,
        item_type=ItemType.ipfs,
        signature="sig3",
        size=1000,
        content={
            "type": "TEST",
            "item_hash": "9abc" * 16,
            "item_type": ItemType.ipfs.value,
        },
        status_value=MessageStatus.REMOVING,
    )
    legacy_message_status = MessageStatusDb(
        item_hash=legacy_message_hash,
        status=MessageStatus.REMOVING,
        reception_time=now,
    )

    # Message concurrently recovered: the GC still lists it (denormalized
    # status_value lags at REMOVING) but message_status is already PROCESSED,
    # so the guarded flip must not happen.
    recovered_message_hash = "mnop" * 16
    recovered_message = MessageDb(
        item_hash=recovered_message_hash,
        sender="0xsender4",
        chain=Chain.ETH,
        type=MessageType.store,
        time=now,
        item_type=ItemType.ipfs,
        signature="sig4",
        size=1000,
        content={
            "type": "TEST",
            "item_hash": "def0" * 16,
            "item_type": ItemType.ipfs.value,
        },
        status_value=MessageStatus.REMOVING,
    )
    recovered_message_status = MessageStatusDb(
        item_hash=recovered_message_hash,
        status=MessageStatus.PROCESSED,
        reception_time=now,
    )

    with session_factory() as session:
        session.add_all(
            [
                store_message,
                store_file,
                store_message_status,
                store_removed_message,
                pinned_message,
                pinned_file,
                pinned_file_pin,
                pinned_message_status,
                legacy_message,
                legacy_message_status,
                recovered_message,
                recovered_message_status,
            ]
        )
        session.commit()

        yield {
            "removable_message": store_message_hash,
            "pinned_message": pinned_message_hash,
            "legacy_message": legacy_message_hash,
            "recovered_message": recovered_message_hash,
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

        # The legacy message (no size snapshot) should also flip to REMOVED
        legacy_status = get_message_status(
            session=session, item_hash=fixture_removing_messages["legacy_message"]
        )
        assert legacy_status is not None
        assert legacy_status.status == MessageStatus.REMOVED

        # The removal record of the removable message keeps its size
        # snapshot, gets the billing metadata copied and removed_at stamped,
        # and the messages row is deleted (forgotten-style)
        removed_message = get_removed_message(
            session=session, item_hash=fixture_removing_messages["removable_message"]
        )
        assert removed_message is not None
        assert removed_message.size == 1000
        assert removed_message.removed_at is not None
        assert removed_message.type == MessageType.store
        assert removed_message.sender == "0xsender1"
        assert removed_message.time is not None
        # No explicit payment field on the message: absence means hold
        assert removed_message.payment_type == "hold"
        assert (
            session.get(MessageDb, fixture_removing_messages["removable_message"])
            is None
        )

        # The legacy message gets a removal record without size
        legacy_removed_message = get_removed_message(
            session=session, item_hash=fixture_removing_messages["legacy_message"]
        )
        assert legacy_removed_message is not None
        assert legacy_removed_message.size is None
        assert legacy_removed_message.removed_at is not None
        assert legacy_removed_message.sender == "0xsender3"
        assert (
            session.get(MessageDb, fixture_removing_messages["legacy_message"]) is None
        )

        # The pinned message stays REMOVING: no removal record, row alive
        pinned_removed_message = get_removed_message(
            session=session, item_hash=fixture_removing_messages["pinned_message"]
        )
        assert pinned_removed_message is None
        assert (
            session.get(MessageDb, fixture_removing_messages["pinned_message"])
            is not None
        )

        # The concurrently recovered message must not reappear as removed:
        # the guarded flip did not happen, so no phantom removal record,
        # message_status stays PROCESSED and the messages row survives
        recovered_status = get_message_status(
            session=session, item_hash=fixture_removing_messages["recovered_message"]
        )
        assert recovered_status is not None
        assert recovered_status.status == MessageStatus.PROCESSED

        recovered_removed_message = get_removed_message(
            session=session, item_hash=fixture_removing_messages["recovered_message"]
        )
        assert recovered_removed_message is None
        assert (
            session.get(MessageDb, fixture_removing_messages["recovered_message"])
            is not None
        )


@pytest.mark.asyncio
async def test_check_removing_messages_rolls_back_flip_on_stamp_failure(
    session_factory: DbSessionFactory,
    gc: GarbageCollector,
    fixture_removing_messages,
    monkeypatch,
):
    """
    A failure between the REMOVING->REMOVED flip and the snapshot/deletion
    must roll back the whole per-message savepoint: a REMOVED message
    without removal record would be invisible to date-windowed queries.
    """

    def failing_remove_message(**_kwargs):
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(
        "aleph.services.storage.garbage_collector.remove_message",
        failing_remove_message,
    )

    # Must not raise: per-message errors are logged and skipped
    await gc._check_and_update_removing_messages()

    with session_factory() as session:
        # The status flip was rolled back together with the failed snapshot
        removable_status = get_message_status(
            session=session, item_hash=fixture_removing_messages["removable_message"]
        )
        assert removable_status is not None
        assert removable_status.status == MessageStatus.REMOVING

        # The messages row is untouched
        message = session.get(MessageDb, fixture_removing_messages["removable_message"])
        assert message is not None
        assert message.status_value == MessageStatus.REMOVING

        # No removed_at was stamped on the phase-1 snapshot record
        removed_message = get_removed_message(
            session=session, item_hash=fixture_removing_messages["removable_message"]
        )
        assert removed_message is not None
        assert removed_message.removed_at is None


@pytest.mark.asyncio
async def test_check_and_update_removing_vprogram_message(
    session_factory: DbSessionFactory, gc: GarbageCollector
):
    """V-PROGRAMs ride the generic (non-STORE) removal path: no file-pin
    gate, REMOVING -> REMOVED in one GC pass, billing metadata (owner,
    credit payment type) copied onto the removal record and the messages
    row deleted."""
    now = utc_now()
    vprogram_hash = ItemHash("beef" * 16)

    vprogram_message = MessageDb(
        item_hash=vprogram_hash,
        sender="0xsender1",
        chain=Chain.ETH,
        type=MessageType.v_program,
        time=now,
        item_type=ItemType.inline,
        signature="sig-vprogram",
        size=500,
        # The GC never parses content; owner and payment_type are derived
        # from it by the MessageDb constructor. V-Programs are credit-only.
        content={
            "address": "0xsender1",
            "time": now.timestamp(),
            "payment": {"type": "credit"},
        },
        status_value=MessageStatus.REMOVING,
    )
    vprogram_status = MessageStatusDb(
        item_hash=vprogram_hash,
        status=MessageStatus.REMOVING,
        reception_time=now,
    )

    with session_factory() as session:
        session.add_all([vprogram_message, vprogram_status])
        session.commit()

    await gc._check_and_update_removing_messages()

    with session_factory() as session:
        status = get_message_status(session=session, item_hash=vprogram_hash)
        assert status is not None
        assert status.status == MessageStatus.REMOVED

        removed_message = get_removed_message(session=session, item_hash=vprogram_hash)
        assert removed_message is not None
        assert removed_message.type == MessageType.v_program
        assert removed_message.sender == "0xsender1"
        assert removed_message.owner == "0xsender1"
        assert removed_message.payment_type == "credit"
        assert removed_message.removed_at is not None

        # The messages row is deleted at removal, mirroring forgotten messages
        assert session.get(MessageDb, vprogram_hash) is None
