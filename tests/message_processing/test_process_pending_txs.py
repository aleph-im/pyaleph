import json
import os
from typing import Dict, List

import pytest
from aleph.jobs import handle_pending_tx
from aleph.model.pending import PendingMessage
from bson.objectid import ObjectId
from pymongo import DeleteOne


def load_fixture_messages(fixture: str) -> List[Dict]:
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    with open(os.path.join(fixtures_dir, fixture)) as f:
        return json.load(f)["content"]["messages"]


# TODO: try to replace this fixture by a get_json fixture. Currently, the pinning
# of the message content gets in the way in the real get_chaindata_messages function.
async def get_fixture_chaindata_messages(
    pending_tx_content, pending_tx_context, seen_ids: List[str]
) -> List[Dict]:
    return load_fixture_messages(f"{pending_tx_content['content']}.json")


@pytest.mark.asyncio
async def test_process_pending_tx(mocker, test_db):
    mocker.patch("aleph.jobs.get_chaindata_messages", get_fixture_chaindata_messages)

    pending_tx = {
        "_id": ObjectId("624ee76595d0a7ca46f4392d"),
        "content": {
            "protocol": "aleph-offchain",
            "version": 1,
            "content": "test-data-pending-tx-messages",
        },
        "context": {
            "chain_name": "ETH",
            "tx_hash": "0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
            "time": 1632835747,
            "height": 13314512,
            "publisher": "0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
        },
    }

    actions_list = []
    seen_ids = []
    await handle_pending_tx(
        pending=pending_tx, actions_list=actions_list, seen_ids=seen_ids
    )

    assert len(actions_list) == 1
    action = actions_list[0]
    assert isinstance(action, DeleteOne)
    assert action._filter == {"_id": pending_tx["_id"]}

    fixture_messages = load_fixture_messages(f"{pending_tx['content']['content']}.json")
    pending_messages = [m async for m in PendingMessage.collection.find()]

    assert len(pending_messages) == len(fixture_messages)
    fixture_messages_by_hash = {m["item_hash"]: m for m in fixture_messages}

    for pending in pending_messages:
        pending_message = pending["message"]
        expected_message = fixture_messages_by_hash[pending_message["item_hash"]]

        # TODO: currently, the pending TX job modifies the time of the message.
        del expected_message["time"]
        assert set(expected_message.items()).issubset(set(pending_message.items()))
