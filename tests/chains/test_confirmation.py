import json
from typing import Dict

import pytest

from aleph.chains.common import process_one_message
from aleph.model.messages import CappedMessage, Message


MESSAGE = {
    "chain": "ETH",
    "sender": "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64",
    "type": "POST",
    "channel": "TEST",
    "item_type": "inline",
    "size": 70,
    "time": 1646123806,
    "item_content": '{"body": "Top 10 cutest Kodiak bears that will definitely murder you"}',
    "item_hash": "fd14aaae5693710fae42fc58049f80ba7abdbf0cce00eb73e585bc89907eaad8",
    "signature": "0xccb6a4c7e2a709accf941463a93064a9f34ea1d03b17fe9d117c80fb0878ee0a2f284af4afb37de187a1116c0cec5b3a8da89b40d5281919dbeebdffc50c86c71c",
}


def remove_id_key(mongodb_object: Dict) -> Dict:
    return {k: v for k, v in mongodb_object.items() if k != "_id"}


@pytest.mark.asyncio
async def test_confirm_message(test_db):
    """
    Tests the flow of confirmation for real-time messages.
    1. We process the message unconfirmed, as if it came through the P2P
       network
    2. We process the message again, this time as it it was fetched from
       on-chain data.

    We then check that the message was correctly updated in the messages
    collection. We also check the capped messages collection used for
    the websockets.
    """

    item_hash = MESSAGE["item_hash"]
    content = json.loads(MESSAGE["item_content"])

    await process_one_message(MESSAGE)
    message_in_db = await Message.collection.find_one({"item_hash": item_hash})

    assert message_in_db is not None
    assert message_in_db["content"]["body"] == content["body"]
    assert not message_in_db["confirmed"]

    capped_message_in_db = await CappedMessage.collection.find_one(
        {"item_hash": item_hash}
    )
    assert capped_message_in_db is not None
    assert remove_id_key(message_in_db) == remove_id_key(capped_message_in_db)

    # Now, confirm the message
    chain_name, tx_hash, height = "ETH", "123", 8000
    await process_one_message(
        MESSAGE, chain_name=chain_name, tx_hash=tx_hash, height=height
    )

    message_in_db = await Message.collection.find_one({"item_hash": item_hash})

    assert message_in_db is not None
    assert message_in_db["confirmed"]
    assert {"chain": chain_name, "hash": tx_hash, "height": height} in message_in_db[
        "confirmations"
    ]

    capped_message_after_confirmation = await CappedMessage.collection.find_one(
        {"item_hash": item_hash}
    )

    assert capped_message_after_confirmation == capped_message_in_db
    assert not capped_message_after_confirmation["confirmed"]
    assert "confirmations" not in capped_message_after_confirmation


@pytest.mark.asyncio
async def test_process_confirmed_message(test_db):
    """
    Tests that a confirmed message coming directly from the on-chain integration flow
    is processed correctly, and that we get one confirmed entry in messages and one
    in capped messages.
    """

    item_hash = MESSAGE["item_hash"]

    # Now, confirm the message
    chain_name, tx_hash, height = "ETH", "123", 8000
    await process_one_message(
        MESSAGE, chain_name=chain_name, tx_hash=tx_hash, height=height
    )

    message_in_db = await Message.collection.find_one({"item_hash": item_hash})

    assert message_in_db is not None
    assert message_in_db["confirmed"]

    expected_confirmations = [{"chain": chain_name, "hash": tx_hash, "height": height}]
    assert message_in_db["confirmations"] == expected_confirmations

    capped_message_in_db = await CappedMessage.collection.find_one(
        {"item_hash": item_hash}
    )

    assert remove_id_key(message_in_db) == remove_id_key(capped_message_in_db)
    assert capped_message_in_db["confirmed"]
    assert capped_message_in_db["confirmations"] == expected_confirmations
