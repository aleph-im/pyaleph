import datetime as dt

import pytest
import pytz

from aleph.model.scheduled_deletions import ScheduledDeletion, ScheduledDeletionInfo


@pytest.mark.asyncio
async def test_insert_deletion(mocker, test_db):
    deletion = ScheduledDeletionInfo(
        filename="test-filename",
        delete_by=pytz.utc.localize(dt.datetime(2022, 1, 1)),
    )
    await ScheduledDeletion.insert(deletion)

    deletion_from_db = await ScheduledDeletion.collection.find_one(
        filter={"filename": deletion.filename}
    )
    assert deletion_from_db is not None
    assert deletion_from_db["filename"] == deletion.filename
    assert deletion_from_db["delete_by"] == deletion.delete_by


@pytest.mark.asyncio
async def test_list_files_to_delete(mocker, test_db):
    deletions = (
        ScheduledDeletionInfo(
            filename="test-file-1",
            delete_by=pytz.utc.localize(dt.datetime(2022, 1, 1, 5, 0, 0)),
        ),
        ScheduledDeletionInfo(
            filename="test-file-2",
            delete_by=pytz.utc.localize(dt.datetime(2022, 1, 1, 0, 0, 0)),
        ),
        ScheduledDeletionInfo(
            filename="test-file-3",
            delete_by=pytz.utc.localize(dt.datetime(2023, 1, 1, 0, 0, 0)),
        ),
    )

    for deletion in deletions:
        await ScheduledDeletion.insert(deletion)

    # Just after the first message
    deletions_from_db = [
        d
        async for d in ScheduledDeletion.files_to_delete(
            delete_by=deletions[1].delete_by
        )
    ]
    assert len(deletions_from_db) == 1

    deletion = deletions_from_db[0]
    assert deletion.filename == deletions[1].filename

    # Some date in the future
    deletions_from_db = [
        d
        async for d in ScheduledDeletion.files_to_delete(
            delete_by=pytz.utc.localize(dt.datetime(2024, 1, 1))
        )
    ]
    # Check that the items were sorted
    assert deletions_from_db[0].filename == deletions[1].filename
    assert deletions_from_db[1].filename == deletions[0].filename
    assert deletions_from_db[2].filename == deletions[2].filename

    # Some date before all the scheduled deletions
    deletions_from_db = [
        d
        async for d in ScheduledDeletion.files_to_delete(
            delete_by=pytz.utc.localize(dt.datetime(2000, 1, 1))
        )
    ]
    assert len(deletions_from_db) == 0
