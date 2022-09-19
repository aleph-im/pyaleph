from collections import defaultdict
from typing import Dict, List

import pytest
from bson.objectid import ObjectId
from pymongo import DeleteOne, InsertOne

from aleph.jobs.process_pending_txs import handle_pending_tx
from aleph.model.pending import PendingMessage, PendingTX
from .load_fixtures import load_fixture_messages


# TODO: try to replace this fixture by a get_json fixture. Currently, the pinning
#       of the message content gets in the way in the real get_chaindata_messages function.
async def get_fixture_chaindata_messages(
    pending_tx_content, pending_tx_context, seen_ids: List[str]
) -> List[Dict]:
    return load_fixture_messages(f"{pending_tx_content['content']}.json")


@pytest.mark.asyncio
async def test_process_pending_tx(mocker, test_db):
    mocker.patch(
        "aleph.jobs.process_pending_txs.get_chaindata_messages",
        get_fixture_chaindata_messages,
    )

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

    seen_ids = []
    db_operations = await handle_pending_tx(pending_tx=pending_tx, seen_ids=seen_ids)

    db_operations_by_collection = defaultdict(list)
    for op in db_operations:
        db_operations_by_collection[op.collection].append(op)

    assert set(db_operations_by_collection.keys()) == {PendingMessage, PendingTX}

    pending_tx_ops = db_operations_by_collection[PendingTX]
    assert len(pending_tx_ops) == 1
    assert isinstance(pending_tx_ops[0].operation, DeleteOne)
    assert pending_tx_ops[0].operation._filter == {"_id": pending_tx["_id"]}

    pending_msg_ops = db_operations_by_collection[PendingMessage]
    fixture_messages = load_fixture_messages(f"{pending_tx['content']['content']}.json")

    assert len(pending_msg_ops) == len(fixture_messages)
    fixture_messages_by_hash = {msg["item_hash"]: msg for msg in fixture_messages}

    for pending_msg_op in pending_msg_ops:
        assert isinstance(pending_msg_op.operation, InsertOne)
        pending_message = pending_msg_op.operation._doc["message"]
        expected_message = fixture_messages_by_hash[
            pending_msg_op.operation._doc["message"]["item_hash"]
        ]
        # TODO: currently, the pending TX job modifies the time of the message.
        del pending_message["time"]
        assert set(pending_message.items()).issubset(set(expected_message.items()))
