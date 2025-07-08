import datetime as dt
from typing import Final

import pytest
import pytz

from aleph.db.accessors.files import (
    get_file_tag,
    is_pinned_file,
    refresh_file_tag,
    upsert_file_tag,
)
from aleph.db.models import MessageFilePinDb, StoredFileDb, TxFilePinDb
from aleph.types.db_session import AsyncDbSessionFactory
from aleph.types.files import FileTag, FileType


@pytest.mark.asyncio
async def test_is_pinned_file(session_factory: AsyncDbSessionFactory):
    async def is_pinned(_session_factory, _file_hash) -> bool:
        async with _session_factory() as _session:
            return await is_pinned_file(session=_session, file_hash=_file_hash)

    file = StoredFileDb(
        hash="QmTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPPdWd",
        size=27,
        type=FileType.FILE,
    )

    async with session_factory() as session:
        session.add(file)
        await session.commit()

    # We check for equality with True/False to determine that the function does indeed
    # return a boolean value
    assert await is_pinned(session_factory, file.hash) is False

    async with session_factory() as session:
        session.add(
            TxFilePinDb(
                file_hash=file.hash, tx_hash="1234", created=dt.datetime(2020, 1, 1)
            )
        )
        await session.commit()

    assert await is_pinned(session_factory, file.hash) is True


@pytest.mark.asyncio
async def test_upsert_file_tag(session_factory: AsyncDbSessionFactory):
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

    async with session_factory() as session:
        session.add(original_file)
        session.add(new_version)
        await session.commit()

    async with session_factory() as session:
        await upsert_file_tag(
            session=session,
            tag=tag,
            owner=owner,
            file_hash=original_file.hash,
            last_updated=original_datetime,
        )
        await session.commit()

        file_tag_db = await get_file_tag(session=session, tag=tag)
        assert file_tag_db is not None
        assert file_tag_db.owner == owner
        assert file_tag_db.file_hash == original_file.hash
        assert file_tag_db.last_updated == original_datetime

    # Update the tag
    async with session_factory() as session:
        new_version_datetime = pytz.utc.localize(dt.datetime(2022, 1, 1))
        await upsert_file_tag(
            session=session,
            tag=tag,
            owner=owner,
            file_hash=new_version.hash,
            last_updated=new_version_datetime,
        )
        await session.commit()

        file_tag_db = await get_file_tag(session=session, tag=tag)
        assert file_tag_db is not None
        assert file_tag_db.owner == owner
        assert file_tag_db.file_hash == new_version.hash
        assert file_tag_db.last_updated == new_version_datetime

    # Try to update the tag to an older version and check it has no effect
    async with session_factory() as session:
        await upsert_file_tag(
            session=session,
            tag=tag,
            owner=owner,
            file_hash=original_file.hash,
            last_updated=original_datetime,
        )
        await session.commit()

        file_tag_db = await get_file_tag(session=session, tag=tag)
        assert file_tag_db is not None
        assert file_tag_db.owner == owner
        assert file_tag_db.file_hash == new_version.hash
        assert file_tag_db.last_updated == new_version_datetime


@pytest.mark.asyncio
async def test_refresh_file_tag(session_factory: AsyncDbSessionFactory):
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

    async with session_factory() as session:
        session.add_all([first_pin, second_pin])
        await session.commit()

    async with session_factory() as session:
        await refresh_file_tag(session=session, tag=tag)
        await session.commit()

        file_tag_db = await get_file_tag(session=session, tag=tag)
        assert file_tag_db
        assert file_tag_db.file_hash == second_pin.file_hash
        assert file_tag_db.last_updated == second_pin.created
