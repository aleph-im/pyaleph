import datetime as dt

import pytest
import pytz

from aleph.jobs.garbage_collector import delete_file
from aleph.model.hashes import (
    get_value as get_gridfs_file,
    set_value as store_gridfs_file,
)
from aleph.model.scheduled_deletions import ScheduledDeletionInfo


@pytest.mark.asyncio
async def test_delete_gridfs_file(mocker, test_db):
    """
    Checks that the garbage collector can delete a file from local (GridFS) storage.
    """

    file_content = b"Some data stored using GridFS+MongoDB"
    filename = "file_to_delete"

    await store_gridfs_file(filename, file_content)

    # Check that the file was properly inserted
    db_content = await get_gridfs_file(filename)
    assert db_content == file_content

    deletion = ScheduledDeletionInfo(
        filename=filename,
        delete_by=pytz.utc.localize(dt.datetime(2022, 1, 1, 0, 0, 0)),
    )

    await delete_file(deletion)
    db_content = await get_gridfs_file(filename)
    assert db_content is None


@pytest.mark.asyncio
async def test_delete_nonexisting_file(mocker, test_db):
    """
    Checks that the delete_file function does not raise an exception
    if the file does not exist.
    """

    filename = "the_mystery_file"

    # Check that the file indeed does not exist
    db_content = await get_gridfs_file(filename)
    assert db_content is None

    deletion = ScheduledDeletionInfo(
        filename=filename,
        delete_by=pytz.utc.localize(dt.datetime(2022, 1, 1, 0, 0, 0)),
    )

    await delete_file(deletion)
