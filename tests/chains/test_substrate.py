import json

import pytest

from aleph.chains.substrate import SubstrateConnector
from aleph.schemas.pending_messages import parse_message

TEST_MESSAGE = '{"chain": "DOT", "channel": "TEST", "sender": "5CGNMKCscqN2QNcT7Jtuz23ab7JUxh8wTEtXhECZLJn5vCGX", "type": "AGGREGATE", "item_type": "inline", "time": 1601913525.231501, "item_content": "{\\"key\\":\\"test\\",\\"address\\":\\"5CGNMKCscqN2QNcT7Jtuz23ab7JUxh8wTEtXhECZLJn5vCGX\\",\\"content\\":{\\"a\\":1},\\"time\\":1601913525.231498}", "item_hash": "bfbc94fae6336d52ab65a4d907d399a0c16222bd944b3815faa08ad0e039ca1d", "signature": "{\\"curve\\": \\"sr25519\\", \\"data\\": \\"0x1ccefb257e89b4e3ecb7d71c8dc1d6e286290b9e32d2a11bf3f9d425c5790f4bff0b324dc774d20a13e38a340d1a48fada71fb0c68690c3adb8f0cc695b0eb83\\"}", "content": {"key": "test", "address": "5CGNMKCscqN2QNcT7Jtuz23ab7JUxh8wTEtXhECZLJn5vCGX", "content": {"a": 1}, "time": 1601913525.231498}}'


@pytest.mark.asyncio
async def test_verify_signature_real():
    message_dict = json.loads(TEST_MESSAGE)
    raw_message = parse_message(message_dict)

    connector = SubstrateConnector()
    result = await connector.verify_signature(raw_message)
    assert result is True


@pytest.mark.asyncio
async def test_verify_signature_bad_json():
    message_dict = json.loads(TEST_MESSAGE)
    raw_message = parse_message(message_dict)
    raw_message.signature = "baba"

    connector = SubstrateConnector()
    result = await connector.verify_signature(raw_message)
    assert result is False


@pytest.mark.asyncio
async def test_verify_signature_no_data():
    message_dict = json.loads(TEST_MESSAGE)
    raw_message = parse_message(message_dict)

    signature = json.loads(raw_message.signature)
    del signature["data"]

    raw_message.signature = json.dumps(signature)
    connector = SubstrateConnector()
    result = await connector.verify_signature(raw_message)
    assert result is False
