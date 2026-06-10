"""
Tests for the IPNS branch of StoreMessageHandler.

Drives StoreMessageHandler directly (with a mocked StorageService / IpfsService)
to avoid the full message pipeline, following the same style as
test_process_stores.py.
"""

import datetime as dt
import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aleph_message.models import (
    Chain,
    ItemHash,
    ItemType,
    MessageType,
    StoreContent,
)
from configmanager import Config
from sqlalchemy import select

from aleph.db.accessors.files import get_ipns_file_pin
from aleph.db.accessors.ipns import get_ipns_record, upsert_ipns_record
from aleph.db.models import (
    GracePeriodFilePinDb,
    MessageDb,
    StoredFileDb,
)
from aleph.db.models.account_costs import AccountCostsDb
from aleph.handlers.content.store import IpfsFileStats, StoreMessageHandler
from aleph.services.ipfs.service import IpnsRecordInfo, IpnsResolutionError
from aleph.toolkit.constants import (
    DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.ipns import IpnsStatus
from aleph.types.message_status import FileUnavailable, InvalidMessageFormat

# ---------------------------------------------------------------------------
# Test constants
# ---------------------------------------------------------------------------

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"
OWNER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
CID_V1 = "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB"
CID_V2 = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
VALIDITY = dt.datetime(2028, 6, 9, tzinfo=dt.timezone.utc)

# A timestamp that is after the cost cutoff so messages always require payment.
MSG_TIME = STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1

# Fake record bytes (content does not matter; we mock verify_ipns_record)
FAKE_RECORD = b"\x00\x01\x02fake_record"
FAKE_RECORD_B64 = __import__("base64").b64encode(FAKE_RECORD).decode()

# Minimal valid-looking message hashes (64 hex chars = storage type according
# to item_type_from_hash, but that function is not called on the *message*
# item_hash — only on the *content* item_hash which is an IPNS name).
MSG_HASH_1 = "a" * 64
MSG_HASH_2 = "b" * 64
MSG_HASH_3 = "c" * 64


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ipns_content(
    *,
    name: str = IPNS_NAME,
    owner: str = OWNER,
    max_size_mib: int = 100,
    ipns_record: str | None = FAKE_RECORD_B64,
    time: float = MSG_TIME,
) -> StoreContent:
    """Build a StoreContent with item_type=ipns."""
    return StoreContent(
        address=owner,
        time=time,
        item_type=ItemType.ipns,
        item_hash=ItemHash(name),
        max_size_mib=max_size_mib,
        ipns_record=ipns_record,
    )


def _make_message_db(
    mocker,
    *,
    item_hash: str = MSG_HASH_1,
    content: StoreContent | None = None,
) -> MagicMock:
    """Return a minimal MessageDb-spec mock with parsed_content wired up."""
    if content is None:
        content = _make_ipns_content()
    msg = mocker.MagicMock(spec=MessageDb)
    msg.item_hash = item_hash
    msg.type = MessageType.store
    msg.chain = Chain.ETH
    msg.sender = OWNER
    msg.signature = None
    msg.item_type = ItemType.inline
    msg.item_content = json.dumps(content.model_dump())
    msg.parsed_content = content
    msg.time = timestamp_to_datetime(content.time)
    msg.channel = Channel("TEST")
    msg.confirmations = []
    return msg


def _make_handler(
    mocker, *, ipfs_service=None
) -> tuple[StoreMessageHandler, MagicMock]:
    """Return (handler, ipfs_service_mock)."""
    if ipfs_service is None:
        ipfs_service = mocker.AsyncMock()
    storage_service = mocker.MagicMock()
    storage_service.ipfs_service = ipfs_service
    handler = StoreMessageHandler(
        storage_service=storage_service,
        grace_period=24,
        max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
    )
    return handler, ipfs_service


def _setup_stats_mock(ipfs_service, *, size: int, file_type: FileType = FileType.FILE):
    """Wire up _get_file_stats_from_ipfs to return a fixed IpfsFileStats."""
    ipfs_service.storage_client.files.stat = AsyncMock(
        return_value={
            "Type": "file" if file_type == FileType.FILE else "dir",
            "Size": size,
            "CumulativeSize": size,
        }
    )


def _record_info(
    cid: str, sequence: int = 1, validity: dt.datetime = VALIDITY
) -> IpnsRecordInfo:
    return IpnsRecordInfo(value_cid=cid, sequence=sequence, validity=validity)


def _seed_ipns_record(
    session,
    *,
    name: str = IPNS_NAME,
    owner: str = OWNER,
    item_hash: str = MSG_HASH_1,
    resolved_cid: str = CID_V1,
    sequence: int = 1,
) -> None:
    """Insert an ipns_records row and the corresponding files row."""
    session.add(
        StoredFileDb(hash=resolved_cid, size=4 * 1024 * 1024, type=FileType.FILE)
    )
    session.flush()
    upsert_ipns_record(
        session=session,
        name=name,
        owner=owner,
        item_hash=item_hash,
        record=FAKE_RECORD,
        record_sequence=sequence,
        record_validity=VALIDITY,
        max_size_mib=100,
        resolved_cid=resolved_cid,
        last_resolved=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        status=IpnsStatus.OK,
        created=timestamp_to_datetime(MSG_TIME),
    )


def _insert_message_row(session, item_hash: str, content: StoreContent) -> None:
    """Insert a minimal MessageDb row so AccountCostsDb FK is satisfied."""
    session.add(
        MessageDb(
            item_hash=item_hash,
            type=MessageType.store,
            chain=Chain.ETH,
            sender=OWNER,
            signature=None,
            item_type=ItemType.inline,
            item_content=json.dumps(content.model_dump()),
            content=content.model_dump(),
            time=timestamp_to_datetime(content.time),
            channel=None,
            size=len(json.dumps(content.model_dump())),
            status_value="processed",
            reception_time=timestamp_to_datetime(content.time),
        )
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ipns_publish(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    Happy-path publish: message with ipns_record, verify OK, stats within quota.
    After fetch+process: ipns_records row (resolved_cid=CID_V1, sequence=1, status=OK),
    IpnsFilePinDb exists pointing at CID_V1.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V1, sequence=1)
    )
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()
    _setup_stats_mock(ipfs_service, size=4 * 1024 * 1024)

    content = _make_ipns_content()
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=4 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            await handler.fetch_related_content(session=session, message=message)
            await handler.process(session=session, messages=[message])
            session.commit()

        with session_factory() as session:
            record_db = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
            assert record_db is not None
            assert record_db.resolved_cid == CID_V1
            assert record_db.record_sequence == 1
            assert record_db.status == IpnsStatus.OK
            assert record_db.item_hash == MSG_HASH_1

            pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
            assert pin is not None
            assert pin.file_hash == CID_V1
            assert pin.item_hash == MSG_HASH_1


@pytest.mark.asyncio
async def test_ipns_update_repoints_pin(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    Update: existing registration at seq=1/CID_V1. New message verifies to seq=2/CID_V2.
    Pin re-points to CID_V2, old CID_V1 gets a grace-period pin.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V2, sequence=2)
    )
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()

    with session_factory() as session:
        # Seed registration at sequence=1 pointing at CID_V1.
        _seed_ipns_record(
            session, item_hash=MSG_HASH_1, resolved_cid=CID_V1, sequence=1
        )
        # Also seed a CID_V1 IPNS pin (as if the first message was already processed).
        from aleph.db.accessors.files import insert_ipns_file_pin

        insert_ipns_file_pin(
            session=session,
            file_hash=CID_V1,
            owner=OWNER,
            item_hash=MSG_HASH_1,
            name=IPNS_NAME,
            created=timestamp_to_datetime(MSG_TIME),
        )
        # Also seed CID_V2 as a stored file (to satisfy FK when upsert_file is called).
        session.add(StoredFileDb(hash=CID_V2, size=5 * 1024 * 1024, type=FileType.FILE))
        session.commit()

    content = _make_ipns_content(max_size_mib=100)
    message2 = _make_message_db(mocker, item_hash=MSG_HASH_2, content=content)

    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=5 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            await handler.fetch_related_content(session=session, message=message2)
            await handler.process(session=session, messages=[message2])
            session.commit()

    with session_factory() as session:
        # Pin must now point at CID_V2.
        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin is not None
        assert pin.file_hash == CID_V2
        assert pin.item_hash == MSG_HASH_2

        # CID_V1 has no more active pins, so it should have a grace-period pin.
        cid_v1_file = session.execute(
            select(StoredFileDb).where(StoredFileDb.hash == CID_V1)
        ).scalar_one_or_none()
        assert cid_v1_file is not None
        grace_pins = [
            p for p in cid_v1_file.pins if isinstance(p, GracePeriodFilePinDb)
        ]
        assert len(grace_pins) == 1


@pytest.mark.asyncio
async def test_ipns_stale_sequence_noop(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    Stale sequence: stored sequence=2, incoming verifies to sequence=1.
    fetch_related_content returns without error; ipns_records unchanged.
    """
    handler, ipfs_service = _make_handler(mocker)
    # Verify returns sequence=1 (stale vs stored sequence=2).
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V1, sequence=1)
    )
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()

    with session_factory() as session:
        _seed_ipns_record(
            session, item_hash=MSG_HASH_1, resolved_cid=CID_V1, sequence=2
        )
        session.commit()

    content = _make_ipns_content()
    message = _make_message_db(mocker, item_hash=MSG_HASH_2, content=content)

    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=4 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            # Should not raise.
            await handler.fetch_related_content(session=session, message=message)
            session.commit()

    with session_factory() as session:
        # Record still points at original message.
        record_db = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record_db is not None
        assert record_db.item_hash == MSG_HASH_1
        assert record_db.record_sequence == 2

    # pin_add must not have been called for the stale update.
    ipfs_service.pin_add.assert_not_called()


@pytest.mark.asyncio
async def test_ipns_track_only(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    Track-only flow: message has no ipns_record field.
    resolve_ipns_record returns FAKE_RECORD; rest proceeds like publish.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.resolve_ipns_record = AsyncMock(return_value=FAKE_RECORD)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V1, sequence=1)
    )
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()

    # No ipns_record field in the content.
    content = _make_ipns_content(ipns_record=None)
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=4 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            await handler.fetch_related_content(session=session, message=message)
            await handler.process(session=session, messages=[message])
            session.commit()

    # resolve_ipns_record should have been called with the name.
    ipfs_service.resolve_ipns_record.assert_called_once_with(
        IPNS_NAME, timeout=mock_config.ipfs.ipns.resolve_timeout.value
    )

    with session_factory() as session:
        record_db = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record_db is not None
        assert record_db.resolved_cid == CID_V1


@pytest.mark.asyncio
async def test_ipns_track_only_dht_failure(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    Track-only: resolve_ipns_record raises IpnsResolutionError.
    fetch_related_content must raise FileUnavailable.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.resolve_ipns_record = AsyncMock(
        side_effect=IpnsResolutionError("dht down")
    )

    content = _make_ipns_content(ipns_record=None)
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with session_factory() as session:
        with pytest.raises(FileUnavailable):
            await handler.fetch_related_content(session=session, message=message)


@pytest.mark.asyncio
async def test_ipns_invalid_record(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    verify_ipns_record raises InvalidIpnsRecordError.
    fetch_related_content must raise InvalidMessageFormat.
    """
    from aleph.services.ipfs.service import InvalidIpnsRecordError

    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        side_effect=InvalidIpnsRecordError("bad sig")
    )

    content = _make_ipns_content()
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with session_factory() as session:
        with pytest.raises(InvalidMessageFormat):
            await handler.fetch_related_content(session=session, message=message)


@pytest.mark.asyncio
async def test_ipns_expired_record(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    verify_ipns_record returns a validity in the past.
    fetch_related_content must raise InvalidMessageFormat.
    """
    past_validity = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)

    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V1, sequence=1, validity=past_validity)
    )

    content = _make_ipns_content()
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with session_factory() as session:
        with pytest.raises(InvalidMessageFormat):
            await handler.fetch_related_content(session=session, message=message)


@pytest.mark.asyncio
async def test_ipns_over_quota(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    File stats size > max_size_mib * MiB.
    fetch_related_content must raise InvalidMessageFormat.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V1, sequence=1)
    )
    ipfs_service.pin_add = AsyncMock()

    content = _make_ipns_content(max_size_mib=100)
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    # 200 MiB file vs 100 MiB quota.
    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=200 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            with pytest.raises(InvalidMessageFormat):
                await handler.fetch_related_content(session=session, message=message)


@pytest.mark.asyncio
async def test_ipns_forget_teardown(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    After a publish (test 1 flow), forget_message deletes ipns_records and pin;
    CID_V1 gets a grace-period pin.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V1, sequence=1)
    )
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()

    content = _make_ipns_content()
    message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=4 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            await handler.fetch_related_content(session=session, message=message)
            await handler.process(session=session, messages=[message])
            session.commit()

        with session_factory() as session:
            await handler.forget_message(session=session, message=message)
            session.commit()

    with session_factory() as session:
        # ipns_records row deleted.
        record_db = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record_db is None

        # IPNS pin deleted; CID_V1 should have a grace-period pin.
        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin is None

        cid_v1_file = session.execute(
            select(StoredFileDb).where(StoredFileDb.hash == CID_V1)
        ).scalar_one_or_none()
        assert cid_v1_file is not None
        grace_pins = [
            p for p in cid_v1_file.pins if isinstance(p, GracePeriodFilePinDb)
        ]
        assert len(grace_pins) == 1


@pytest.mark.asyncio
async def test_ipns_forget_superseded_noop(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
):
    """
    After an update (test 2 scenario), forgetting the FIRST (superseded) message
    leaves the registration and pin intact.
    """
    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()

    with session_factory() as session:
        # State: registration now points at MSG_HASH_2 / CID_V2.
        session.add(StoredFileDb(hash=CID_V2, size=5 * 1024 * 1024, type=FileType.FILE))
        session.flush()
        upsert_ipns_record(
            session=session,
            name=IPNS_NAME,
            owner=OWNER,
            item_hash=MSG_HASH_2,  # current winning message
            record=FAKE_RECORD,
            record_sequence=2,
            record_validity=VALIDITY,
            max_size_mib=100,
            resolved_cid=CID_V2,
            last_resolved=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
            status=IpnsStatus.OK,
            created=timestamp_to_datetime(MSG_TIME),
        )
        from aleph.db.accessors.files import insert_ipns_file_pin

        insert_ipns_file_pin(
            session=session,
            file_hash=CID_V2,
            owner=OWNER,
            item_hash=MSG_HASH_2,
            name=IPNS_NAME,
            created=timestamp_to_datetime(MSG_TIME),
        )
        session.commit()

    # Forget the OLD (superseded) message 1.
    content = _make_ipns_content()
    old_message = _make_message_db(mocker, item_hash=MSG_HASH_1, content=content)

    with session_factory() as session:
        await handler.forget_message(session=session, message=old_message)
        session.commit()

    with session_factory() as session:
        # Registration intact, pointing at MSG_HASH_2.
        record_db = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record_db is not None
        assert record_db.item_hash == MSG_HASH_2

        # Pin intact.
        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin is not None
        assert pin.file_hash == CID_V2


@pytest.mark.asyncio
async def test_ipns_cost_rows_migrate_on_update(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    On update: AccountCostsDb rows for the superseded message are deleted;
    the new registration remains intact.
    """
    from aleph_message.models import PaymentType

    handler, ipfs_service = _make_handler(mocker)
    ipfs_service.verify_ipns_record = AsyncMock(
        return_value=_record_info(CID_V2, sequence=2)
    )
    ipfs_service.pin_add = AsyncMock()
    ipfs_service.put_ipns_record = AsyncMock()

    content1 = _make_ipns_content()
    content2 = _make_ipns_content()

    with session_factory() as session:
        # Insert message 1 row (needed for FK on AccountCostsDb).
        _insert_message_row(session, item_hash=MSG_HASH_1, content=content1)
        session.flush()

        # Seed registration pointing at message 1.
        _seed_ipns_record(
            session, item_hash=MSG_HASH_1, resolved_cid=CID_V1, sequence=1
        )
        session.flush()

        # Seed a cost row for message 1.
        session.add(
            AccountCostsDb(
                owner=OWNER,
                item_hash=MSG_HASH_1,
                type=CostType.STORAGE,
                name="storage",
                ref=None,
                payment_type=PaymentType.hold,
                cost_hold=Decimal("1.0"),
                cost_stream=Decimal("0"),
                cost_credit=Decimal("0"),
            )
        )
        # Seed the IPNS file pin for message 1.
        from aleph.db.accessors.files import insert_ipns_file_pin

        insert_ipns_file_pin(
            session=session,
            file_hash=CID_V1,
            owner=OWNER,
            item_hash=MSG_HASH_1,
            name=IPNS_NAME,
            created=timestamp_to_datetime(MSG_TIME),
        )
        # Seed CID_V2 as a stored file.
        session.add(StoredFileDb(hash=CID_V2, size=5 * 1024 * 1024, type=FileType.FILE))
        session.commit()

    message2 = _make_message_db(mocker, item_hash=MSG_HASH_2, content=content2)

    with patch(
        "aleph.handlers.content.store._get_file_stats_from_ipfs",
        new=AsyncMock(
            return_value=IpfsFileStats(size=5 * 1024 * 1024, file_type=FileType.FILE)
        ),
    ):
        with session_factory() as session:
            await handler.fetch_related_content(session=session, message=message2)
            await handler.process(session=session, messages=[message2])
            session.commit()

    with session_factory() as session:
        # Message 1 cost rows deleted.
        costs_msg1 = (
            session.execute(
                select(AccountCostsDb).where(AccountCostsDb.item_hash == MSG_HASH_1)
            )
            .scalars()
            .all()
        )
        assert len(costs_msg1) == 0

        # Registration updated to message 2.
        record_db = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record_db is not None
        assert record_db.item_hash == MSG_HASH_2
