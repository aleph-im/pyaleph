import datetime as dt
from typing import Any, Mapping, Sequence

import pytest
import pytz
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models import (
    ForgottenMessageDb,
    MessageDb,
    MessageStatusDb,
    PendingMessageDb,
    RejectedMessageDb,
)
from aleph.schemas.api.messages import (
    ForgottenMessageStatus,
    PendingMessageStatus,
    ProcessedMessageStatus,
    RejectedMessageStatus,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import ErrorCode, MessageStatus

MESSAGE_URI = "/api/v0/messages/{}"


RECEPTION_DATETIME = pytz.utc.localize(dt.datetime(2023, 1, 1))


@pytest.fixture
def fixture_messages_with_status(
    session_factory: DbSessionFactory,
) -> Mapping[MessageStatus, Sequence[Any]]:

    pending_messages = [
        PendingMessageDb(
            item_hash="9ee49b5457baf686aa9b8d9941009b99c921b01873a611f3b972c2103bf4ef55",
            chain=Chain.ETH,
            sender="0x59f1f0464540073Bc70edAab069496366c128115",
            signature="0x359de7173cd9a8804fdd88490f3b8f61cc372e3a334c78311d91ee509b90e885185abed16a585198f5b8103e1158f09a083b26b5e5a7492c5b172c75c9e15af71c",
            item_type=ItemType.storage,
            type=MessageType.aggregate,
            item_content=None,
            content={
                "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "time": 1645794065.439,
                "aggregates": [],
                "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
                "reason": "None",
            },
            time=timestamp_to_datetime(1645794080),
            reception_time=RECEPTION_DATETIME,
            channel=Channel("TEST"),
            retries=1,
            next_attempt=dt.datetime(2023, 1, 1),
            check_message=True,
            fetched=False,
        ),
        PendingMessageDb(
            item_hash="88a045b90b48c590748a607690fdf85b94b7be4d63940bf4835828b3254b4265",
            chain=Chain.ETH,
            sender="0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
            signature="0x2acfc49c4709a97bb63fd63277304e54b474912785a48efdcd0ee4571f4b42a8730fda00a6c7bdaf20c42f92c8ff5449824a668ab1897359f4a0e503c335b8a21b",
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            item_content='{"address":"0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4","time":1648215810.245091,"type":"POST"}',
            content={
                "address": "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
                "time": 1648215810.245091,
                "type": "POST",
            },
            time=timestamp_to_datetime(1645794080),
            reception_time=RECEPTION_DATETIME,
            channel=Channel("INTEGRATION_TESTS"),
            retries=2,
            next_attempt=dt.datetime(2030, 1, 1),
            check_message=True,
            fetched=True,
        ),
    ]

    processed_messages = [
        MessageDb(
            item_hash="e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5",
            chain=Chain.ETH,
            sender="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            signature="0xabfa661aab1a9f58955940ea213387de4773f8b1f244c2236cd4ac5ba7bf2ba902e17074bc4b289ba200807bb40951f4249668b055dc15af145b8842ecfad0601c",
            item_type=ItemType.storage,
            type=MessageType.forget,
            item_content=None,
            content={
                "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "time": 1645794065.439,
                "aggregates": [],
                "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
                "reason": "None",
            },
            size=154,
            time=timestamp_to_datetime(1645794065.439),
            channel=Channel("TEST"),
        )
    ]

    forgotten_messages = [
        ForgottenMessageDb(
            item_hash="QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF",
            chain=Chain.ETH,
            sender="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            signature="some-signature",
            item_type=ItemType.inline,
            type=MessageType.store,
            time=timestamp_to_datetime(1645794000),
            channel=Channel("TEST"),
            forgotten_by=[
                "e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5"
            ],
        )
    ]

    rejected_messages = [
        RejectedMessageDb(
            item_hash="3946eb27511391a04a599d56f0f44c0a0787797b8b2274be8b8cf2c38244a93a",
            message={
                "time": 1672671290.836,
                "type": "FORGET",
                "chain": "ETH",
                "sender": "0xD498D9267b68Da05dd986B00f6fEF42f46e134Da",
                "channel": "TEST",
                "content": {
                    "time": 1672671290.836,
                    "hashes": [],
                    "reason": "None",
                    "address": "0xD498D9267b68Da05dd986B00f6fEF42f46e134Da",
                },
                "item_hash": "3946eb27511391a04a599d56f0f44c0a0787797b8b2274be8b8cf2c38244a93a",
                "item_type": "inline",
                "signature": "0xe36ff18a728d2666f7ba0519d745c43de7f6e2ce41406cf49650183c844014c53b265ae48ceefd5e3d82cce3ce3e8e2e49bd2e83bb031a3677c2b4eed7ff08f01c",
                "item_content": '{"address":"0xD498D9267b68Da05dd986B00f6fEF42f46e134Da","time":1672671290.836,"hashes":[],"reason":"None"}',
            },
            error_code=ErrorCode.FORGET_NO_TARGET,
            details=None,
        )
    ]

    messages_dict: Mapping[MessageStatus, Sequence[Any]] = {
        MessageStatus.PENDING: pending_messages,
        MessageStatus.PROCESSED: processed_messages,
        MessageStatus.FORGOTTEN: forgotten_messages,
        MessageStatus.REJECTED: rejected_messages,
    }

    with session_factory() as session:
        for status, messages in messages_dict.items():
            for message in messages:
                session.add(message)
                session.add(
                    MessageStatusDb(
                        item_hash=message.item_hash,
                        status=status,
                        reception_time=RECEPTION_DATETIME,
                    )
                )
        session.commit()

    return messages_dict


@pytest.mark.asyncio
async def test_get_processed_message_status(
    fixture_messages_with_status: Mapping[MessageStatus, Sequence[Any]], ccn_api_client
):
    for processed_message in fixture_messages_with_status[MessageStatus.PROCESSED]:
        response = await ccn_api_client.get(
            MESSAGE_URI.format(processed_message.item_hash)
        )
        assert response.status == 200, await response.text()
        response_json = await response.json()
        parsed_response = ProcessedMessageStatus.parse_obj(response_json)
        assert parsed_response.status == MessageStatus.PROCESSED
        assert parsed_response.item_hash == processed_message.item_hash
        assert parsed_response.reception_time == RECEPTION_DATETIME

        assert parsed_response.message.item_hash == processed_message.item_hash
        assert parsed_response.message.sender == processed_message.sender
        assert parsed_response.message.signature == processed_message.signature
        assert parsed_response.message.channel == processed_message.channel
        assert parsed_response.message.item_type == processed_message.item_type
        assert parsed_response.message.item_content == processed_message.item_content
        assert response_json["message"]["content"] == processed_message.content
        assert parsed_response.message.time == processed_message.time


@pytest.mark.asyncio
async def test_get_rejected_message_status(
    fixture_messages_with_status: Mapping[MessageStatus, Sequence[Any]], ccn_api_client
):
    for rejected_message in fixture_messages_with_status[MessageStatus.REJECTED]:
        response = await ccn_api_client.get(
            MESSAGE_URI.format(rejected_message.item_hash)
        )
        assert response.status == 200, await response.text()
        response_json = await response.json()
        parsed_response = RejectedMessageStatus.parse_obj(response_json)
        assert parsed_response.status == MessageStatus.REJECTED
        assert parsed_response.item_hash == rejected_message.item_hash
        assert parsed_response.reception_time == RECEPTION_DATETIME

        assert parsed_response.message["item_hash"] == rejected_message.item_hash
        assert parsed_response.message == rejected_message.message
        assert parsed_response.error_code == rejected_message.error_code
        assert parsed_response.details == rejected_message.details

        # Check that the traceback is not included in the response
        assert "traceback" not in response_json


@pytest.mark.asyncio
async def test_get_forgotten_message_status(
    fixture_messages_with_status: Mapping[MessageStatus, Sequence[Any]], ccn_api_client
):
    for forgotten_message in fixture_messages_with_status[MessageStatus.FORGOTTEN]:
        response = await ccn_api_client.get(
            MESSAGE_URI.format(forgotten_message.item_hash)
        )
        assert response.status == 200, await response.text()
        response_json = await response.json()
        parsed_response = ForgottenMessageStatus.parse_obj(response_json)
        assert parsed_response.status == MessageStatus.FORGOTTEN
        assert parsed_response.item_hash == forgotten_message.item_hash
        assert parsed_response.reception_time == RECEPTION_DATETIME

        assert parsed_response.message.item_hash == forgotten_message.item_hash
        assert parsed_response.message.sender == forgotten_message.sender
        assert parsed_response.message.signature == forgotten_message.signature
        assert parsed_response.message.channel == forgotten_message.channel
        assert parsed_response.message.item_type == forgotten_message.item_type
        assert parsed_response.message.time == forgotten_message.time

        assert parsed_response.forgotten_by == forgotten_message.forgotten_by

        # Check that the content is not included in the response, somehow
        assert "item_content" not in response_json
        assert "content" not in response_json


@pytest.mark.asyncio
async def test_get_pending_message_status(
    fixture_messages_with_status: Mapping[MessageStatus, Sequence[Any]], ccn_api_client
):
    for processed_message in fixture_messages_with_status[MessageStatus.PENDING]:
        response = await ccn_api_client.get(
            MESSAGE_URI.format(processed_message.item_hash)
        )
        assert response.status == 200, await response.text()
        response_json = await response.json()
        parsed_response = PendingMessageStatus.parse_obj(response_json)
        assert parsed_response.status == MessageStatus.PENDING
        assert parsed_response.item_hash == processed_message.item_hash
        assert parsed_response.reception_time == RECEPTION_DATETIME

        assert len(parsed_response.messages) == 1
        message = parsed_response.messages[0]

        assert message.item_hash == processed_message.item_hash
        assert message.sender == processed_message.sender
        assert message.signature == processed_message.signature
        assert message.channel == processed_message.channel
        assert message.item_type == processed_message.item_type
        assert message.item_content == processed_message.item_content
        assert response_json["messages"][0].get("content") == processed_message.content
        assert message.time == processed_message.time

        assert "retries" not in response_json["messages"][0]
        assert "fetched" not in response_json["messages"][0]
        assert "check_message" not in response_json["messages"][0]
