import datetime as dt
from typing import List

import pytest
import pytest_asyncio
from sqlalchemy import select

from aleph.db.accessors.files import get_file
from aleph.db.models import (
    StoredFileDb,
    MessageFilePinDb,
    GracePeriodFilePinDb,
    TxFilePinDb,
)
from aleph.services.storage.engine import StorageEngine
from aleph.services.storage.garbage_collector import GarbageCollector
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.files import FileType


@pytest.fixture
def gc(
    session_factory: DbSessionFactory, test_storage_service: StorageService
) -> GarbageCollector:
    return GarbageCollector(
        session_factory=session_factory, storage_service=test_storage_service
    )


@pytest_asyncio.fixture
async def fixture_files(
    session_factory: DbSessionFactory, test_storage_service: StorageService
):
    files = [
        StoredFileDb(
            hash="dead" * 16,
            size=120,
            type=FileType.FILE,
            pins=[
                MessageFilePinDb(
                    created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                    owner="0xbadbabe",
                    item_hash="bebe" * 16,
                )
            ],
        ),
        StoredFileDb(
            hash="bc" * 32,
            size=1024,
            type=FileType.FILE,
            pins=[
                TxFilePinDb(
                    created=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                    tx_hash="some-tx-hash",
                )
            ],
        ),
        StoredFileDb(
            hash="bad0" * 16,
            size=12000000,
            type=FileType.DIRECTORY,
            pins=[
                GracePeriodFilePinDb(
                    created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                    delete_by=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                )
            ],
        ),
        StoredFileDb(
            hash="bad1" * 16,
            size=345666,
            type=FileType.FILE,
            pins=[
                GracePeriodFilePinDb(
                    created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                    delete_by=dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc),
                )
            ],
        ),
        StoredFileDb(
            hash="bad2" * 16,
            size=1234567,
            type=FileType.FILE,
        ),
    ]

    for file in files:
        await test_storage_service.storage_engine.write(
            filename=file.hash, content=b"test"
        )

    with session_factory() as session:
        session.add_all(files)
        session.commit()

        yield files


async def assert_file_is_deleted(
    session: DbSession, storage_engine: StorageEngine, file_hash: str
):
    file_db = get_file(session=session, file_hash=file_hash)
    assert file_db is None

    content = await storage_engine.read(filename=file_hash)
    assert content is None


async def assert_file_exists(
    session: DbSession, storage_engine: StorageEngine, file_hash: str
):
    file_db = get_file(session=session, file_hash=file_hash)
    assert file_db

    content = await storage_engine.read(filename=file_hash)
    assert content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "gc_run_datetime",
    [
        dt.datetime(2040, 1, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2023, 6, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
    ],
)
async def test_garbage_collector_collect(
    session_factory: DbSessionFactory,
    gc: GarbageCollector,
    fixture_files: List[StoredFileDb],
    gc_run_datetime: dt.datetime,
):
    with session_factory() as session:
        await gc.collect(datetime=gc_run_datetime)
        session.commit()

        storage_engine = gc.storage_service.storage_engine
        for fixture_file in fixture_files:
            if not fixture_file.pins:
                await assert_file_is_deleted(session, storage_engine, fixture_file.hash)
            else:
                pins = fixture_file.pins
                # Files with an outdated grace period pin should be deleted
                if (
                    all(isinstance(pin, GracePeriodFilePinDb) for pin in pins)
                    and max(pin.delete_by for pin in pins) < gc_run_datetime  # type: ignore[attr-defined]
                ):
                    await assert_file_is_deleted(
                        session, storage_engine, fixture_file.hash
                    )

                else:
                    await assert_file_exists(session, storage_engine, fixture_file.hash)
