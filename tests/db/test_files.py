import datetime as dt
from typing import Final

import pytest
import pytz

from aleph.db.accessors.files import (
    is_pinned_file,
    upsert_file_tag,
    get_file_tag,
    refresh_file_tag,
)
from aleph.db.models import TxFilePinDb, StoredFileDb, MessageFilePinDb
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType, FileTag


@pytest.mark.asyncio
async def test_is_pinned_file(session_factory: DbSessionFactory):
    def is_pinned(_session_factory, _file_hash) -> bool:
        with _session_factory() as _session:
            return is_pinned_file(session=_session, file_hash=_file_hash)

    file = StoredFileDb(
        hash="QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd",
        size=27,
        type=FileType.FILE,
    )

    with session_factory() as session:
        session.add(file)
        session.commit()

    # We check for equality with True/False to determine that the function does indeed
    # return a boolean value
    assert is_pinned(session_factory, file.hash) is False

    with session_factory() as session:
        session.add(
            TxFilePinDb(
                file_hash=file.hash, tx_hash="1234", created=dt.datetime(2020, 1, 1)
            )
        )
        session.commit()

    assert is_pinned(session_factory, file.hash) is True


@pytest.mark.asyncio
async def test_upsert_file_tag(session_factory: DbSessionFactory):
    original_file = StoredFileDb(
        hash="QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd",
        size=32,
        type=FileType.FILE,
    )
    new_version = StoredFileDb(
        hash="QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWe",
        size=413,
        type=FileType.FILE,
    )

    original_datetime = pytz.utc.localize(dt.datetime(2020, 1, 1))
    tag = FileTag("aleph/custom-tag")
    owner = "aleph"

    with session_factory() as session:
        session.add(original_file)
        session.add(new_version)
        session.commit()

    with session_factory() as session:
        upsert_file_tag(
            session=session,
            tag=tag,
            owner=owner,
            file_hash=original_file.hash,
            last_updated=original_datetime,
        )
        session.commit()

        file_tag_db = get_file_tag(session=session, tag=tag)
        assert file_tag_db is not None
        assert file_tag_db.owner == owner
        assert file_tag_db.file_hash == original_file.hash
        assert file_tag_db.last_updated == original_datetime

    # Update the tag
    with session_factory() as session:
        new_version_datetime = pytz.utc.localize(dt.datetime(2022, 1, 1))
        upsert_file_tag(
            session=session,
            tag=tag,
            owner=owner,
            file_hash=new_version.hash,
            last_updated=new_version_datetime,
        )
        session.commit()

        file_tag_db = get_file_tag(session=session, tag=tag)
        assert file_tag_db is not None
        assert file_tag_db.owner == owner
        assert file_tag_db.file_hash == new_version.hash
        assert file_tag_db.last_updated == new_version_datetime

    # Try to update the tag to an older version and check it has no effect
    with session_factory() as session:
        upsert_file_tag(
            session=session,
            tag=tag,
            owner=owner,
            file_hash=original_file.hash,
            last_updated=original_datetime,
        )
        session.commit()

        file_tag_db = get_file_tag(session=session, tag=tag)
        assert file_tag_db is not None
        assert file_tag_db.owner == owner
        assert file_tag_db.file_hash == new_version.hash
        assert file_tag_db.last_updated == new_version_datetime


@pytest.mark.asyncio
async def test_refresh_file_tag(session_factory: DbSessionFactory):
    files = [
        StoredFileDb(
            hash="QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd",
            size=123,
            type=FileType.FILE,
        ),
        StoredFileDb(
            hash="QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWe",
            size=678,
            type=FileType.FILE,
        ),
    ]

    owner = "aleph"
    tag = FileTag("4d1052267dfb2aff9d7b5a70cd004e100fe2fccfb492b24e3dcd1b8da9f3ae73")

    first_pin: Final[MessageFilePinDb] = MessageFilePinDb(
        file=files[0],
        created=pytz.utc.localize(dt.datetime(2020, 1, 1)),
        item_hash=tag,
        owner=owner,
    )
    second_pin = MessageFilePinDb(
        file=files[1],
        created=pytz.utc.localize(dt.datetime(2022, 1, 1)),
        item_hash="e5eb60fd1adfc4d3b9dc7c16ab00e20a50cd690fdf0108fb8e7899a94c578770",
        ref=tag,
        owner=owner,
    )

    with session_factory() as session:
        session.add_all([first_pin, second_pin])
        session.commit()

    with session_factory() as session:
        refresh_file_tag(session=session, tag=tag)
        session.commit()

        file_tag_db = get_file_tag(session=session, tag=tag)
        assert file_tag_db
        assert file_tag_db.file_hash == second_pin.file_hash
        assert file_tag_db.last_updated == second_pin.created
