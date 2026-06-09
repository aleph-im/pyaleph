import datetime as dt

import pytest

from aleph.db.accessors.files import (
    get_ipns_file_pin,
    insert_ipns_file_pin,
    update_ipns_file_pin,
    upsert_file,
)
from aleph.db.accessors.ipns import (
    delete_ipns_record,
    get_all_ipns_records,
    get_ipns_record,
    get_ipns_records_by_name,
    get_ipns_records_by_owner,
    upsert_ipns_record,
)
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.ipns import IpnsStatus

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"
OWNER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
CID = "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB"
MSG_HASH = "7f2d09b2c4e1a8f3d6b5c2e9a1f4d7b8e3c6a9f2d5b8e1c4a7f0d3b6e9c2a5f8"
NOW = dt.datetime(2026, 6, 10, tzinfo=dt.timezone.utc)


def _upsert(session, sequence=1, item_hash=MSG_HASH, resolved_cid=CID):
    upsert_ipns_record(
        session=session,
        name=IPNS_NAME,
        owner=OWNER,
        item_hash=item_hash,
        record=b"\x0a\x01raw",
        record_sequence=sequence,
        record_validity=NOW + dt.timedelta(days=365),
        max_size_mib=100,
        resolved_cid=resolved_cid,
        last_resolved=NOW,
        status=IpnsStatus.OK,
        created=NOW,
    )


@pytest.mark.asyncio
async def test_upsert_and_get_ipns_record(session_factory: DbSessionFactory):
    with session_factory() as session:
        upsert_file(session, file_hash=CID, size=1024, file_type=FileType.FILE)
        _upsert(session)
        session.commit()

    with session_factory() as session:
        record = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record is not None
        assert record.record_sequence == 1
        assert record.resolved_cid == CID

    with session_factory() as session:
        _upsert(session, sequence=2)
        session.commit()

    with session_factory() as session:
        record = get_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        assert record.record_sequence == 2

        assert len(list(get_ipns_records_by_name(session, name=IPNS_NAME))) == 1
        assert len(list(get_ipns_records_by_owner(session, owner=OWNER))) == 1
        assert len(list(get_all_ipns_records(session))) == 1


@pytest.mark.asyncio
async def test_delete_ipns_record(session_factory: DbSessionFactory):
    with session_factory() as session:
        upsert_file(session, file_hash=CID, size=1024, file_type=FileType.FILE)
        _upsert(session)
        session.commit()
        delete_ipns_record(session, name=IPNS_NAME, owner=OWNER)
        session.commit()
        assert get_ipns_record(session, name=IPNS_NAME, owner=OWNER) is None


@pytest.mark.asyncio
async def test_ipns_file_pin_lifecycle(session_factory: DbSessionFactory):
    cid2 = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
    msg2 = "8a3e10c3d5f2b9a4e7c6d3f0b2a5e8c1d4b7f0a3e6c9d2b5a8f1e4c7d0b3a6f9"
    with session_factory() as session:
        upsert_file(session, file_hash=CID, size=1024, file_type=FileType.FILE)
        upsert_file(session, file_hash=cid2, size=2048, file_type=FileType.FILE)
        insert_ipns_file_pin(
            session=session,
            file_hash=CID,
            owner=OWNER,
            item_hash=MSG_HASH,
            name=IPNS_NAME,
            created=NOW,
        )
        session.commit()

        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin is not None
        assert pin.file_hash == CID

        update_ipns_file_pin(
            session=session,
            name=IPNS_NAME,
            owner=OWNER,
            file_hash=cid2,
            item_hash=msg2,
        )
        session.commit()
        pin = get_ipns_file_pin(session, name=IPNS_NAME, owner=OWNER)
        assert pin.file_hash == cid2
        assert pin.item_hash == msg2
