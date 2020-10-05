import pytest
import json

import aleph.chains
from aleph.chains.substrate import verify_signature

TEST_MESSAGE = '{"chain": "DOT", "channel": "TEST", "sender": "5CGNMKCscqN2QNcT7Jtuz23ab7JUxh8wTEtXhECZLJn5vCGX", "type": "AGGREGATE", "time": 1601913525.231501, "item_content": "{\\"key\\":\\"test\\",\\"address\\":\\"5CGNMKCscqN2QNcT7Jtuz23ab7JUxh8wTEtXhECZLJn5vCGX\\",\\"content\\":{\\"a\\":1},\\"time\\":1601913525.231498}", "item_hash": "bfbc94fae6336d52ab65a4d907d399a0c16222bd944b3815faa08ad0e039ca1d", "signature": "{\\"curve\\": \\"sr25519\\", \\"data\\": \\"0x1ccefb257e89b4e3ecb7d71c8dc1d6e286290b9e32d2a11bf3f9d425c5790f4bff0b324dc774d20a13e38a340d1a48fada71fb0c68690c3adb8f0cc695b0eb83\\"}", "content": {"key": "test", "address": "5CGNMKCscqN2QNcT7Jtuz23ab7JUxh8wTEtXhECZLJn5vCGX", "content": {"a": 1}, "time": 1601913525.231498}}'

@pytest.mark.asyncio
async def test_verify_signature_real():
    message = json.loads(TEST_MESSAGE)
    result = await verify_signature(message)
    assert result == True
    
@pytest.mark.asyncio
async def test_verify_signature_nonexistent():
    result = await verify_signature({
        'chain': 'CHAIN',
        'sender': 'SENDER',
        'type': 'TYPE',
        'item_hash': 'ITEM_HASH'
    })
    assert result == False
    
@pytest.mark.asyncio
async def test_verify_signature_bad_json():
    result = await verify_signature({
        'chain': 'CHAIN',
        'sender': 'SENDER',
        'type': 'TYPE',
        'item_hash': 'ITEM_HASH',
        'signature': 'baba'
    })
    assert result == False
    
@pytest.mark.asyncio
async def test_verify_signature_no_data():
    message = json.loads(TEST_MESSAGE)
    signature = json.loads(message['signature'])
    del signature['data']
    message['signature'] = json.dumps(signature)
    result = await verify_signature(message)
    assert result == False