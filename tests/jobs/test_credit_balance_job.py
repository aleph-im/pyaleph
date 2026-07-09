import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType

from aleph.db.accessors.messages import get_message_status, get_removed_message
from aleph.db.models.files import (
    FilePinType,
    GracePeriodFilePinDb,
    MessageFilePinDb,
    StoredFileDb,
)
from aleph.db.models.messages import MessageDb, MessageStatusDb, RemovedMessageDb
from aleph.jobs.cron.credit_balance_job import CreditBalanceCronJob
from aleph.toolkit.constants import DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE, MiB
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus

MESSAGE_HASH = "dcba4321" * 8
FILE_HASH = "4321" * 16
FILE_SIZE = 30 * MiB
SENDER = "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"


@pytest.fixture
def credit_balance_job(session_factory: DbSessionFactory) -> CreditBalanceCronJob:
    return CreditBalanceCronJob(
        session_factory=session_factory,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )


def _add_store_message(session, status: MessageStatus) -> None:
    now = utc_now()
    message = MessageDb(
        item_hash=MESSAGE_HASH,
        sender=SENDER,
        chain=Chain.ETH,
        type=MessageType.store,
        time=now,
        item_type=ItemType.ipfs,
        signature="sig",
        size=1000,
        content={
            "address": SENDER,
            "time": now.timestamp(),
            "item_hash": FILE_HASH,
            "item_type": ItemType.ipfs.value,
            "payment": {"type": "credit"},
        },
        status_value=status,
    )
    file = StoredFileDb(hash=FILE_HASH, size=FILE_SIZE, type=FileType.FILE)
    if status == MessageStatus.PROCESSED:
        pin: object = MessageFilePinDb(
            item_hash=MESSAGE_HASH,
            file_hash=FILE_HASH,
            type=FilePinType.MESSAGE,
            owner=SENDER,
            created=now,
        )
    else:
        pin = GracePeriodFilePinDb(
            item_hash=MESSAGE_HASH,
            file_hash=FILE_HASH,
            type=FilePinType.GRACE_PERIOD,
            owner=SENDER,
            created=now,
            delete_by=now,
        )
    message_status = MessageStatusDb(
        item_hash=MESSAGE_HASH,
        status=status,
        reception_time=now,
    )
    session.add_all([message, file, pin, message_status])


@pytest.mark.asyncio
async def test_credit_balance_job_delete_snapshots_removed_message(
    session_factory: DbSessionFactory, credit_balance_job: CreditBalanceCronJob
):
    """Marking a credit STORE for removal snapshots its file size."""
    with session_factory() as session:
        _add_store_message(session, MessageStatus.PROCESSED)
        session.commit()

        await credit_balance_job.delete_messages(session, [ItemHash(MESSAGE_HASH)])
        session.commit()

        removed_message = get_removed_message(session=session, item_hash=MESSAGE_HASH)
        assert removed_message is not None
        assert removed_message.size == FILE_SIZE
        assert removed_message.removed_at is None


@pytest.mark.asyncio
async def test_credit_balance_job_recover_discards_removed_message(
    session_factory: DbSessionFactory, credit_balance_job: CreditBalanceCronJob
):
    """Recovering a REMOVING message discards its removal record."""
    with session_factory() as session:
        _add_store_message(session, MessageStatus.REMOVING)
        session.add(RemovedMessageDb(item_hash=MESSAGE_HASH, size=FILE_SIZE))
        session.commit()

        await credit_balance_job.recover_messages(session, [ItemHash(MESSAGE_HASH)])
        session.commit()

        removed_message = get_removed_message(session=session, item_hash=MESSAGE_HASH)
        assert removed_message is None


@pytest.mark.asyncio
async def test_credit_balance_job_delete_skips_snapshot_without_flip(
    session_factory: DbSessionFactory, credit_balance_job: CreditBalanceCronJob
):
    """
    No removal record is written when the PROCESSED->REMOVING flip does not
    happen (message already on another transition).
    """
    with session_factory() as session:
        _add_store_message(session, MessageStatus.REMOVING)
        session.commit()

        await credit_balance_job.delete_messages(session, [ItemHash(MESSAGE_HASH)])
        session.commit()

        removed_message = get_removed_message(session=session, item_hash=MESSAGE_HASH)
        assert removed_message is None


def _add_finalized_removal(session, removed_at) -> None:
    """Finalized removal: status REMOVED, snapshot present, messages row
    deleted by the garbage collector."""
    session.add(
        MessageStatusDb(
            item_hash=MESSAGE_HASH,
            status=MessageStatus.REMOVED,
            reception_time=utc_now(),
        )
    )
    session.add(
        RemovedMessageDb(item_hash=MESSAGE_HASH, size=FILE_SIZE, removed_at=removed_at)
    )


@pytest.mark.asyncio
async def test_credit_balance_job_recover_keeps_record_after_gc_stamp(
    session_factory: DbSessionFactory, credit_balance_job: CreditBalanceCronJob
):
    """
    A recovery attempt racing with the garbage collector must not destroy a
    finalized removal record: the REMOVING->PROCESSED flip does not happen
    (the message is already REMOVED), so the record survives and no
    messages row reappears.
    """
    removed_at = utc_now()

    with session_factory() as session:
        _add_finalized_removal(session, removed_at)
        session.commit()

        await credit_balance_job.recover_messages(session, [ItemHash(MESSAGE_HASH)])
        session.commit()

        removed_message = get_removed_message(session=session, item_hash=MESSAGE_HASH)
        assert removed_message is not None
        assert removed_message.size == FILE_SIZE
        assert removed_message.removed_at == removed_at

        # The message must not reappear as processed: the guarded flip did
        # not happen and no messages row exists for a finalized removal.
        status = get_message_status(session=session, item_hash=ItemHash(MESSAGE_HASH))
        assert status is not None
        assert status.status == MessageStatus.REMOVED
        assert session.get(MessageDb, MESSAGE_HASH) is None


@pytest.mark.asyncio
async def test_credit_balance_job_delete_keeps_removed_status(
    session_factory: DbSessionFactory, credit_balance_job: CreditBalanceCronJob
):
    """
    A removal attempt on an already-REMOVED message must not flip its
    status back to REMOVING nor write a removal record over the finalized
    one.
    """
    removed_at = utc_now()

    with session_factory() as session:
        _add_finalized_removal(session, removed_at)
        session.commit()

        await credit_balance_job.delete_messages(session, [ItemHash(MESSAGE_HASH)])
        session.commit()

        status = get_message_status(session=session, item_hash=ItemHash(MESSAGE_HASH))
        assert status is not None
        assert status.status == MessageStatus.REMOVED

        removed_message = get_removed_message(session=session, item_hash=MESSAGE_HASH)
        assert removed_message is not None
        assert removed_message.removed_at == removed_at
