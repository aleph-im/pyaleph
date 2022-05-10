import datetime as dt
import json

import pytest

from aleph.chains.common import process_one_message
from aleph.model.hashes import (
    get_value as read_gridfs_file,
    set_value as store_gridfs_file,
)
from aleph.model.scheduled_deletions import ScheduledDeletionInfo, ScheduledDeletion


@pytest.mark.asyncio
async def test_cancel_scheduled_deletion(test_db):
    """
    Test that a file marked for deletion will be preserved once a message
    stores that content.
    """

    store_message = {
        "chain": "ETH",
        "channel": "unit-tests",
        "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
        "type": "STORE",
        "time": 1652126721.497669,
        "item_content": '{"address":"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106","time":1652126721.4974446,"item_type":"storage","item_hash":"5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21","mime_type":"text/plain"}',
        "item_type": "inline",
        "item_hash": "2953f0b52beb79fc0ed1bc455346fdcb530611605e16c636778a0d673d7184af",
        "signature": "0xa10129dd561c1bc93e8655daf09520e9f1694989263e25f330b403ad33563f4b64c9ae18f6cbfb33e8a47a095be7a181b140a369e6205fd04eef55397624a7121b",
    }

    content = json.loads(store_message["item_content"])
    file_hash = content["item_hash"]
    file_content = b"Hello, Aleph.im!\n"

    # Store the file
    await store_gridfs_file(file_hash, file_content)
    await ScheduledDeletion.insert(
        ScheduledDeletionInfo(
            filename=file_hash,
            delete_by=dt.datetime.utcnow() + dt.timedelta(seconds=3600),
        )
    )

    await process_one_message(store_message)

    # Check that the file is no longer marked for deletion
    scheduled_deletion = await ScheduledDeletion.collection.find_one(
        {"filename": file_hash}
    )
    assert scheduled_deletion is None

    # Check that the file is unmodified
    db_file_content = await read_gridfs_file(file_hash)
    assert db_file_content == file_content
