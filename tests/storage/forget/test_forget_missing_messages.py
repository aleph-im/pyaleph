import pytest

from aleph.chains.common import process_one_message, delayed_incoming
from aleph.handlers.forget import handle_forget_message
from aleph.model.messages import Message
from aleph.model.pending import PendingMessage

FORGET_MESSAGE = {
    "chain": "ETH",
    "item_hash": "884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a",
    "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
    "type": "FORGET",
    "channel": "TEST",
    "item_content": '{"address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1639058312.376,"hashes":["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],"reason":"None"}',
    "item_type": "inline",
    "signature": "0x7dc7a45aab12d78367c085799d06ef2e98fce31f76ca06975ce570fe4d92008f66f307bf68ed3ca450d04d4e779776ca13a1e7851cb48915bd390389ae4afd1b1c",
    "size": 172,
    "time": 1639058312.376,
}
FORGET_MESSAGE_CONTENT = {
    "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
    "time": 1639058312.376,
    "hashes": ["e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"],
    "reason": "None",
}


@pytest.mark.asyncio
async def test_handle_forget_missing_message(mocker):
    """
    Tests that the FORGET message handler marks the message for retry if
    the targeted message is not in the database.
    """

    class NoMessageFoundIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            return StopAsyncIteration

    message_mock = mocker.patch("aleph.handlers.forget.Message")
    message_mock.return_value.collection.return_value.find.return_value = (
        NoMessageFoundIterator()
    )
    result = await handle_forget_message(FORGET_MESSAGE, FORGET_MESSAGE_CONTENT)
    assert result is None

    assert message_mock.collection.find.called_once()


@pytest.mark.asyncio
async def test_forget_missing_message_db(mocker, test_db):
    """
    Tests that processing a FORGET message that targets missing messages
    results in the FORGET message remaining in the pending message queue,
    marked for retry.
    """

    result = await handle_forget_message(FORGET_MESSAGE, FORGET_MESSAGE_CONTENT)
    assert result is None

    await delayed_incoming(FORGET_MESSAGE)

    db_id = (
        await PendingMessage.collection.find_one(
            {"message.item_hash": FORGET_MESSAGE["item_hash"]}, {"_id": 1}
        )
    )["_id"]

    await process_one_message(FORGET_MESSAGE, retrying=True, existing_id=db_id)

    pending_message = await PendingMessage.collection.find_one({"_id": db_id})
    assert pending_message is not None
    assert pending_message["retries"] == 1

    # Check that no message was inserted
    message = await Message.collection.find_one(
        {"item_hash": FORGET_MESSAGE["item_hash"]}
    )
    assert message is None
