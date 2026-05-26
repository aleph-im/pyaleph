import datetime as dt

import pytest
import pytz

from aleph.db.accessors.files import insert_message_file_pin
from aleph.db.models import StoredFileDb
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType

OWNER = "0xfilteraddress"
OTHER_OWNER = "0xotheraddress"

FILE_A = "QmaaaTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPP"
FILE_B = "QmbbbTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPP"
FILE_C = "QmcccTm7g1Mh3BhrQPjnedVQ5g67DR7cwhyMN3MvFt1JPP"

STORE_A = "11" * 32
STORE_B = "22" * 32
STORE_C = "33" * 32


@pytest.fixture
def fixture_account_files(session_factory: DbSessionFactory) -> None:
    created = pytz.utc.localize(dt.datetime(2024, 1, 1))
    with session_factory() as session:
        for file_hash, size in [(FILE_A, 10), (FILE_B, 20), (FILE_C, 30)]:
            session.add(StoredFileDb(hash=file_hash, size=size, type=FileType.FILE))
        session.flush()

        insert_message_file_pin(
            session=session,
            file_hash=FILE_A,
            owner=OWNER,
            item_hash=STORE_A,
            ref=None,
            created=created,
        )
        insert_message_file_pin(
            session=session,
            file_hash=FILE_B,
            owner=OWNER,
            item_hash=STORE_B,
            ref=None,
            created=created + dt.timedelta(seconds=1),
        )
        # Same file_hash as FILE_A but pinned by a different owner.
        insert_message_file_pin(
            session=session,
            file_hash=FILE_C,
            owner=OTHER_OWNER,
            item_hash=STORE_C,
            ref=None,
            created=created,
        )
        session.commit()


@pytest.mark.asyncio
async def test_account_files_filter_by_hash_returns_matching_pin(
    ccn_api_client, fixture_account_files
):
    response = await ccn_api_client.get(
        f"/api/v0/addresses/{OWNER}/files",
        params={"file_hash": FILE_A},
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    files = data["files"]
    assert len(files) == 1
    assert files[0]["file_hash"] == FILE_A
    assert files[0]["item_hash"] == STORE_A


@pytest.mark.asyncio
async def test_account_files_filter_by_hash_unknown_returns_404(
    ccn_api_client, fixture_account_files
):
    response = await ccn_api_client.get(
        f"/api/v0/addresses/{OWNER}/files",
        params={"file_hash": "Qm" + "0" * 44},
    )
    assert response.status == 404


@pytest.mark.asyncio
async def test_account_files_filter_by_hash_other_owner_returns_404(
    ccn_api_client, fixture_account_files
):
    # FILE_C exists and is pinned, but by OTHER_OWNER, not OWNER.
    response = await ccn_api_client.get(
        f"/api/v0/addresses/{OWNER}/files",
        params={"file_hash": FILE_C},
    )
    assert response.status == 404
