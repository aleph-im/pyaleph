"""
Tests for the storage API to check that temporary files are properly marked for deletion.
"""


import datetime as dt
import json

import pytest
import pytz
from aiohttp import FormData
from configmanager import Config

from aleph.model import ScheduledDeletion
from aleph.model.hashes import get_value as read_gridfs_file
from aleph.web import create_app


async def check_scheduled_deletion(
    config: Config, file_hash: str, post_datetime: dt.datetime
):
    scheduled_deletion = await ScheduledDeletion.collection.find_one(
        {"filename": file_hash}
    )

    assert scheduled_deletion is not None

    # Check that the file is scheduled for deletion at least after
    # the expected interval.
    delete_interval = config.storage.delete_interval.value
    delete_by = scheduled_deletion["delete_by"]
    assert delete_by >= post_datetime + dt.timedelta(seconds=delete_interval)


@pytest.mark.asyncio
async def test_store_temporary_file(mock_config, test_db, aiohttp_client):
    """
    Checks that the garbage collector schedules temporary files uploaded
    with /storage/add_file for deletion.
    """

    app = create_app()
    app["config"] = mock_config
    client = await aiohttp_client(app)

    file_content = b"Some file I'd like to upload"

    data = FormData()
    data.add_field("file", file_content)

    post_datetime = pytz.utc.localize(dt.datetime.utcnow())
    response = await client.post(f"/api/v0/storage/add_file", data=data)
    assert response.status == 200, await response.text()

    data = await response.json()
    assert data["status"] == "success"
    file_hash = data["hash"]

    db_content = await read_gridfs_file(file_hash)
    assert db_content == file_content

    await check_scheduled_deletion(mock_config, file_hash, post_datetime)


@pytest.mark.asyncio
async def test_store_temporary_json(mock_config, test_db, aiohttp_client):
    """
    Checks that the garbage collector schedules temporary JSON files uploaded
    with /storage/add_json for deletion.
    """

    app = create_app()
    app["config"] = mock_config
    client = await aiohttp_client(app)

    json_content = {
        "title": "A garbage collector for CCNs",
        "body": "Discover the new GC for Aleph CCNs. Deletes all the files, even useful ones!",
    }

    post_datetime = pytz.utc.localize(dt.datetime.utcnow())
    response = await client.post(f"/api/v0/storage/add_json", json=json_content)
    assert response.status == 200, await response.text()

    data = await response.json()
    assert data["status"] == "success"
    file_hash = data["hash"]

    db_content = await read_gridfs_file(file_hash)
    assert json.loads(db_content) == json_content

    await check_scheduled_deletion(mock_config, file_hash, post_datetime)
