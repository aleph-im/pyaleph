"""
Tests for aleph.services.ipns.republisher.IpnsRepublisher.

Five cases:
1. Republish within validity  - put_ipns_record called, last_republished set.
2. Expiry                     - past validity: status EXPIRED, put_ipns_record NOT called.
3. Adopt newer record         - resolve returns seq+1: row updated, pin re-pointed, old CID grace-pinned.
4. Newer record over quota    - stats exceed quota: OVER_QUOTA, resolved_cid unchanged, no grace pin.
5. Resolution failure         - IpnsResolutionError: cycle completes, record still republished.
"""

import datetime as dt

import pytest

from aleph.db.accessors.files import (
    get_ipns_file_pin,
    insert_ipns_file_pin,
    upsert_file,
)
from aleph.db.accessors.ipns import get_ipns_record, upsert_ipns_record
from aleph.db.models.files import GracePeriodFilePinDb
from aleph.services.ipfs.service import IpnsRecordInfo, IpnsResolutionError
from aleph.services.ipns.republisher import IpnsRepublisher
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.ipns import IpnsStatus

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"
OWNER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
ITEM_HASH = "abcd" * 16
OLD_CID = "Qm" + "a" * 44
NEW_CID = "Qm" + "b" * 44
RECORD_BYTES = b"\x0a\x01record"
NEW_RECORD_BYTES = b"\x0a\x02new-record"

FUTURE = dt.datetime(2099, 1, 1, tzinfo=dt.timezone.utc)
PAST = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)
NOW = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


def _seed_record(
    session_factory: DbSessionFactory,
    *,
    validity: dt.datetime,
    record: bytes = RECORD_BYTES,
    sequence: int = 1,
    resolved_cid: str = OLD_CID,
    status: IpnsStatus = IpnsStatus.OK,
    max_size_mib: int = 100,
) -> None:
    with session_factory() as session:
        # Ensure the old CID file exists so resolved_cid FK is satisfied.
        upsert_file(
            session=session,
            file_hash=resolved_cid,
            file_type=FileType.FILE,
            size=1024,
        )
        session.flush()
        upsert_ipns_record(
            session=session,
            name=IPNS_NAME,
            owner=OWNER,
            item_hash=ITEM_HASH,
            record=record,
            record_sequence=sequence,
            record_validity=validity,
            max_size_mib=max_size_mib,
            resolved_cid=resolved_cid,
            last_resolved=NOW,
            status=status,
            created=NOW,
        )
        session.flush()
        insert_ipns_file_pin(
            session=session,
            file_hash=resolved_cid,
            owner=OWNER,
            item_hash=ITEM_HASH,
            name=IPNS_NAME,
            created=NOW,
        )
        session.commit()


def _make_republisher(session_factory, ipfs_service):
    return IpnsRepublisher(
        session_factory=session_factory,
        ipfs_service=ipfs_service,
        grace_period_hours=24,
        stat_timeout=10,
        resolve_timeout=10,
    )


# ---------------------------------------------------------------------------
# Case 1: Republish within validity
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_republish_within_validity(session_factory: DbSessionFactory, mocker):
    _seed_record(session_factory, validity=FUTURE)

    ipfs_service = mocker.MagicMock()
    ipfs_service.put_ipns_record = mocker.AsyncMock(return_value=None)
    # resolve raises so _re_resolve is a no-op
    ipfs_service.resolve_ipns_record = mocker.AsyncMock(
        side_effect=IpnsResolutionError(IPNS_NAME)
    )

    republisher = _make_republisher(session_factory, ipfs_service)
    await republisher.run_cycle()

    ipfs_service.put_ipns_record.assert_awaited_once_with(IPNS_NAME, RECORD_BYTES)

    with session_factory() as session:
        row = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert row is not None
        assert row.last_republished is not None
        assert row.status == IpnsStatus.OK


# ---------------------------------------------------------------------------
# Case 2: Expiry - validity in the past
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expiry_flips_status(session_factory: DbSessionFactory, mocker):
    _seed_record(session_factory, validity=PAST)

    ipfs_service = mocker.MagicMock()
    ipfs_service.put_ipns_record = mocker.AsyncMock(return_value=None)
    ipfs_service.resolve_ipns_record = mocker.AsyncMock(
        side_effect=IpnsResolutionError(IPNS_NAME)
    )

    republisher = _make_republisher(session_factory, ipfs_service)
    await republisher.run_cycle()

    ipfs_service.put_ipns_record.assert_not_awaited()

    with session_factory() as session:
        row = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert row is not None
        assert row.status == IpnsStatus.EXPIRED
        assert row.resolved_cid == OLD_CID


# ---------------------------------------------------------------------------
# Case 3: Adopt newer record (within quota)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_adopt_newer_record(session_factory: DbSessionFactory, mocker):
    _seed_record(session_factory, validity=FUTURE, sequence=1)

    ipfs_service = mocker.MagicMock()
    ipfs_service.put_ipns_record = mocker.AsyncMock(return_value=None)
    ipfs_service.resolve_ipns_record = mocker.AsyncMock(return_value=NEW_RECORD_BYTES)
    ipfs_service.verify_ipns_record = mocker.AsyncMock(
        return_value=IpnsRecordInfo(
            value_cid=NEW_CID,
            sequence=2,
            validity=FUTURE,
        )
    )
    ipfs_service.pin_add = mocker.AsyncMock(return_value=None)

    from aleph.handlers.content.store import IpfsFileStats

    mocker.patch(
        "aleph.services.ipns.republisher._get_file_stats_from_ipfs",
        return_value=IpfsFileStats(size=1024, file_type=FileType.FILE),
    )

    republisher = _make_republisher(session_factory, ipfs_service)
    await republisher.run_cycle()

    ipfs_service.pin_add.assert_awaited_once_with(cid=NEW_CID)

    with session_factory() as session:
        row = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert row is not None
        assert row.resolved_cid == NEW_CID
        assert row.record_sequence == 2
        assert row.record == NEW_RECORD_BYTES
        assert row.status == IpnsStatus.OK

        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin is not None
        assert pin.file_hash == NEW_CID

        # Old CID should be grace-period-pinned (it has no other pin)
        grace_pins = (
            session.query(GracePeriodFilePinDb)
            .filter(GracePeriodFilePinDb.file_hash == OLD_CID)
            .all()
        )
        assert len(grace_pins) == 1
        assert grace_pins[0].delete_by > dt.datetime.now(tz=dt.timezone.utc)


# ---------------------------------------------------------------------------
# Case 4: Newer record over quota
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_newer_record_over_quota(session_factory: DbSessionFactory, mocker):
    _seed_record(
        session_factory, validity=FUTURE, sequence=1, max_size_mib=1
    )  # 1 MiB quota

    ipfs_service = mocker.MagicMock()
    ipfs_service.put_ipns_record = mocker.AsyncMock(return_value=None)
    ipfs_service.resolve_ipns_record = mocker.AsyncMock(return_value=NEW_RECORD_BYTES)
    ipfs_service.verify_ipns_record = mocker.AsyncMock(
        return_value=IpnsRecordInfo(
            value_cid=NEW_CID,
            sequence=2,
            validity=FUTURE,
        )
    )
    ipfs_service.pin_add = mocker.AsyncMock(return_value=None)

    from aleph.handlers.content.store import IpfsFileStats

    # 2 MiB > 1 MiB quota
    mocker.patch(
        "aleph.services.ipns.republisher._get_file_stats_from_ipfs",
        return_value=IpfsFileStats(size=2 * 1024 * 1024, file_type=FileType.FILE),
    )

    republisher = _make_republisher(session_factory, ipfs_service)
    await republisher.run_cycle()

    # pin_add must NOT have been called for the over-quota CID
    ipfs_service.pin_add.assert_not_awaited()

    with session_factory() as session:
        row = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert row is not None
        assert row.status == IpnsStatus.OVER_QUOTA
        # resolved_cid stays at old value (we did not update it)
        assert row.resolved_cid == OLD_CID
        # But sequence and record are updated so republish stays current
        assert row.record_sequence == 2
        assert row.record == NEW_RECORD_BYTES

        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin is not None
        assert pin.file_hash == OLD_CID  # pin not re-pointed

        # No grace-period pin for OLD_CID
        grace_pins = (
            session.query(GracePeriodFilePinDb)
            .filter(GracePeriodFilePinDb.file_hash == OLD_CID)
            .all()
        )
        assert len(grace_pins) == 0


# ---------------------------------------------------------------------------
# Case 5: Resolution failure tolerated
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_resolution_failure_tolerated(session_factory: DbSessionFactory, mocker):
    _seed_record(session_factory, validity=FUTURE)

    ipfs_service = mocker.MagicMock()
    ipfs_service.put_ipns_record = mocker.AsyncMock(return_value=None)
    ipfs_service.resolve_ipns_record = mocker.AsyncMock(
        side_effect=IpnsResolutionError(IPNS_NAME)
    )

    republisher = _make_republisher(session_factory, ipfs_service)
    # Must not raise
    await republisher.run_cycle()

    # republish still happened
    ipfs_service.put_ipns_record.assert_awaited_once_with(IPNS_NAME, RECORD_BYTES)

    with session_factory() as session:
        row = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert row is not None
        assert row.last_republished is not None
        assert row.status == IpnsStatus.OK


# ---------------------------------------------------------------------------
# Case 6: Isolation of per-record failures
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_per_record_failure_isolation(session_factory: DbSessionFactory, mocker):
    """One record whose _re_resolve fails must not prevent processing of the next record."""
    # Seed two records with different names
    IPNS_NAME_1 = IPNS_NAME
    IPNS_NAME_2 = "k51qzi5uqu5dgy0oo3su6gmrqe3typv1qfx4f2nltgaha8t77ai4rlbrwlqj66"

    with session_factory() as session:
        # First record
        upsert_file(
            session=session,
            file_hash=OLD_CID,
            file_type=FileType.FILE,
            size=1024,
        )
        session.flush()
        upsert_ipns_record(
            session=session,
            name=IPNS_NAME_1,
            owner=OWNER,
            item_hash=ITEM_HASH,
            record=RECORD_BYTES,
            record_sequence=1,
            record_validity=FUTURE,
            max_size_mib=100,
            resolved_cid=OLD_CID,
            last_resolved=NOW,
            status=IpnsStatus.OK,
            created=NOW,
        )
        session.flush()
        insert_ipns_file_pin(
            session=session,
            file_hash=OLD_CID,
            owner=OWNER,
            item_hash=ITEM_HASH,
            name=IPNS_NAME_1,
            created=NOW,
        )
        session.flush()

        # Second record (different name, different owner to avoid FK conflicts)
        OWNER_2 = "0xB07B1214bAe0D5ccAA25449C3149c0aC83658875"
        ITEM_HASH_2 = "dcba" * 16
        upsert_file(
            session=session,
            file_hash=OLD_CID,
            file_type=FileType.FILE,
            size=1024,
        )
        session.flush()
        upsert_ipns_record(
            session=session,
            name=IPNS_NAME_2,
            owner=OWNER_2,
            item_hash=ITEM_HASH_2,
            record=RECORD_BYTES,
            record_sequence=1,
            record_validity=FUTURE,
            max_size_mib=100,
            resolved_cid=OLD_CID,
            last_resolved=NOW,
            status=IpnsStatus.OK,
            created=NOW,
        )
        session.flush()
        insert_ipns_file_pin(
            session=session,
            file_hash=OLD_CID,
            owner=OWNER_2,
            item_hash=ITEM_HASH_2,
            name=IPNS_NAME_2,
            created=NOW,
        )
        session.commit()

    # Mock ipfs_service to raise for first name, succeed for second
    ipfs_service = mocker.MagicMock()
    ipfs_service.put_ipns_record = mocker.AsyncMock(return_value=None)

    def resolve_side_effect(name, timeout=None):
        if name == IPNS_NAME_1:
            raise RuntimeError("Simulated _re_resolve failure for first record")
        return NEW_RECORD_BYTES

    ipfs_service.resolve_ipns_record = mocker.AsyncMock(side_effect=resolve_side_effect)
    ipfs_service.verify_ipns_record = mocker.AsyncMock(
        return_value=IpnsRecordInfo(
            value_cid=NEW_CID,
            sequence=2,
            validity=FUTURE,
        )
    )
    ipfs_service.pin_add = mocker.AsyncMock(return_value=None)

    from aleph.handlers.content.store import IpfsFileStats

    mocker.patch(
        "aleph.services.ipns.republisher._get_file_stats_from_ipfs",
        return_value=IpfsFileStats(size=1024, file_type=FileType.FILE),
    )

    republisher = _make_republisher(session_factory, ipfs_service)
    # Must not raise despite first record's _re_resolve failure
    await republisher.run_cycle()

    # Second record should have been adopted successfully
    with session_factory() as session:
        row_1 = get_ipns_record(session, name=IPNS_NAME_1, owner=OWNER)
        assert row_1 is not None
        # First record still has old CID (exception prevented adoption)
        assert row_1.resolved_cid == OLD_CID

        row_2 = get_ipns_record(session, name=IPNS_NAME_2, owner=OWNER_2)
        assert row_2 is not None
        # Second record was adopted successfully despite first record's failure
        assert row_2.resolved_cid == NEW_CID
        assert row_2.record_sequence == 2
        assert row_2.record == NEW_RECORD_BYTES
        assert row_2.status == IpnsStatus.OK
