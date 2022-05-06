"""
Tests that validate the behavior of FORGET messages when multiple users store
the same file.
"""
from pathlib import Path

import pytest

from aleph.chains.common import process_one_message
from aleph.model.hashes import (
    get_value as read_gridfs_file,
    set_value as store_gridfs_file,
)
from aleph.model.messages import Message

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.mark.asyncio
async def test_forget_multiusers_storage(mocker, test_db):
    """
    Tests that a file stored by two different users is not deleted if one of the users
    deletes the content with a forget message.
    """

    file_hash = "05a123fe17aa6addeef5a97d1665878d10f076d84309d5ae674d4bb292b484c3"

    message_user1 = {
        "chain": "ETH",
        "sender": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64",
        "type": "STORE",
        "channel": "TESTS_FORGET",
        "confirmed": False,
        "item_type": "inline",
        "size": 202,
        "time": 1646123806,
        "item_content": '{"address": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64", "time": 1651757380.8522494, "item_type": "storage", "item_hash": "05a123fe17aa6addeef5a97d1665878d10f076d84309d5ae674d4bb292b484c3", "size": 220916, "content_type": "file"}',
        "item_hash": "50635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        "signature": "0x71263de6b8d1ea4c0b028f5892287505f6ee73dfa165d1455ca665ffdf5318955345c193a5df2f5c4eb2185947689d7bf5be36155b00711572fec5f27764625c1b",
    }

    message_user2 = {
        "chain": "ETH",
        "sender": "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
        "type": "STORE",
        "channel": "TESTS_FORGET",
        "confirmed": False,
        "item_type": "inline",
        "size": 202,
        "time": 1646123806,
        "item_content": '{"address": "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4", "time": 1651757416.2203836, "item_type": "storage", "item_hash": "05a123fe17aa6addeef5a97d1665878d10f076d84309d5ae674d4bb292b484c3", "size": 220916, "content_type": "file"}',
        "item_hash": "dbe8199004b052108ec19618f43af1d2baf5c04974d0aec1c4de2d02c44a2483",
        "signature": "0x4c9ef501e1e4f4b0a05c1eebfa1063837a82788f80deeb59808d25ff481c855157dd65102eaa365e33c7572a78d551cf25075f49d00ebb60c8506c0a6647ab761b",
    }

    forget_message_user1 = {
        "chain": "ETH",
        "sender": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64",
        "type": "FORGET",
        "channel": "TESTS_FORGET",
        "item_type": "inline",
        "size": 202,
        "time": 1651757583.497435,
        "item_content": '{"address": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64", "time": 1651757583.4974332, "hashes": ["50635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e"], "reason": "I do not like this file"}',
        "item_hash": "0223e74dbae53b45da6a443fa18fd2a25f88677c82ed2de93f17ab24f78f58cf",
        "signature": "0x6682e797c424c8e5def6758867e25f08279afc3e976dbaaefdb9f650eee18d26595fc4e2f18fd4cdd853558140ecbb824e0ea8d221e12267862903fa904fabee1c",
    }

    # Store the file in the DB to make it accessible to the tests
    with open(FIXTURES_DIR / "forget_multi_users_fixture.txt", "rb") as f:
        file_content = f.read()
    await store_gridfs_file(key=file_hash, value=file_content)

    await process_one_message(message_user1)

    message1_db = await Message.collection.find_one(
        {"item_hash": message_user1["item_hash"]}
    )
    assert message1_db is not None

    await process_one_message(message_user2)

    # Sanity check: check that the file exists
    db_file_data = await read_gridfs_file(file_hash)
    assert db_file_data == file_content

    await process_one_message(forget_message_user1)

    # Check that the message was properly forgotten
    forgotten_message = await Message.collection.find_one(
        {"item_hash": message_user1["item_hash"]}
    )
    assert forgotten_message is not None
    assert forgotten_message["forgotten_by"] == [forget_message_user1["item_hash"]]

    # Check that the message from user 2 is not affected
    message_user2_db = await Message.collection.find_one(
        {"item_hash": message_user2["item_hash"]}
    )
    assert message_user2_db is not None
    assert "forgotten_by" not in message_user2_db
    assert message_user2_db["item_content"] == message_user2["item_content"]

    # Check that the file still exists
    db_file_data = await read_gridfs_file(file_hash)
    assert db_file_data == file_content
