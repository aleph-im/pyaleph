import json
from hashlib import sha256
from urllib.parse import unquote

import pytest
from pydantic.tools import parse_obj_as

from aleph.chains.cosmos import CosmosConnector
from aleph.schemas.pending_messages import PendingPostMessage

TEST_MESSAGE = '{"time": 1737558660.738481, "type": "POST", "chain": "CSDK", "origin": "p2p", "sender": "cosmos1pjekm3szchhl7p2jr39hvzpwy78r5hztqdqasd", "channel": "ALEPH-CLOUDSOLUTIONS", "content": {"time": 1737558660.737648, "type": "test", "address": "cosmos1pjekm3szchhl7p2jr39hvzpwy78r5hztqdqasd", "content": {"body": "my content"}}, "item_hash": "9c7136fe3598b940158b5f0bb5c313e9152b53412db7649618003f6807739684", "item_type": "inline", "signature": {"signature": "vlrNQEokZ0bJu6WuTUH7lwaXQb0iSdLbAfuzGyB3Pp15NzFuKtNw6Fw8d4LOcV/zaVnioWMTeNxaf3Cak99ZiA==", "pub_key": {"type": "tendermint/PubKeySecp256k1", "value": "AnswvteOeBxaZVk+PAp6KZ5RSoNjAQgqSaHcfCMpjebu"}, "account_number": "0", "sequence": "0"}, "item_content":{"address":"cosmos1pjekm3szchhl7p2jr39hvzpwy78r5hztqdqasd","time":1737558660.737648,"content":{"body":"my content"},"type":"test"}}'


@pytest.fixture
def cosmos_message() -> PendingPostMessage:
    message = json.loads(unquote(TEST_MESSAGE))
    message["signature"] = json.dumps(message["signature"])
    message["item_content"] = json.dumps(message["item_content"], separators=(",", ":"))
    return parse_obj_as(PendingPostMessage, message)


@pytest.mark.asyncio
async def test_verify_signature_real(cosmos_message: PendingPostMessage):
    connector = CosmosConnector()
    assert await connector.verify_signature(message=cosmos_message)


@pytest.mark.asyncio
async def test_verify_signature_bad_json():
    connector = CosmosConnector()
    message = parse_obj_as(
        PendingPostMessage,
        {
            "chain": "CSDK",
            "time": 1737558660.737648,
            "sender": "SENDER",
            "type": "POST",
            "item_hash": sha256("ITEM_HASH".encode()).hexdigest(),
            "signature": "baba",
        },
    )
    result = await connector.verify_signature(message)
    assert result is False


@pytest.mark.asyncio
async def test_verify_signature_no_data(cosmos_message: PendingPostMessage):
    connector = CosmosConnector()

    signature = json.loads(str(cosmos_message.signature))
    del signature["signature"]
    cosmos_message.signature = json.dumps(signature)
    result = await connector.verify_signature(cosmos_message)
    assert result is False


@pytest.mark.asyncio
async def test_verify_signature_bad_data(cosmos_message: PendingPostMessage):
    connector = CosmosConnector()

    signature = json.loads(str(cosmos_message.signature))
    signature["signature"] = "1" + signature["signature"]
    cosmos_message.signature = json.dumps(signature)
    result = await connector.verify_signature(cosmos_message)
    assert result is False


@pytest.mark.asyncio
async def test_verify_signature_bad_address(cosmos_message: PendingPostMessage):
    connector = CosmosConnector()

    cosmos_message.sender = "cosmos1rq3rcux05yftlh307gw8khh6xj43nv40aq27f5"
    result = await connector.verify_signature(cosmos_message)
    assert result is False
