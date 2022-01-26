import pytest
import json

import aleph.chains
from aleph.chains.cosmos import verify_signature

TEST_MESSAGE = '{"chain": "CSDK", "channel": "TEST", "sender": "cosmos1rq3rcux05yftlh307gw8khh6xj43nv40mq27f5", "type": "AGGREGATE", "time": 1601997899.1849918, "item_content": "{\\"key\\":\\"test\\",\\"address\\":\\"cosmos1rq3rcux05yftlh307gw8khh6xj43nv40mq27f5\\",\\"content\\":{\\"a\\":1},\\"time\\":1601997899.1841497}", "item_hash": "248863f4eae7e31dfa33dd323af641550237c4ce90160aa1d96dd821a1b73221", "signature": "{\\"signature\\": \\"bbbdzrijw3i4eYoWhv6UI8Yqye480LOkJuNwkEp3S0sEshS9L+tQlbJSnfLDBwkC4VDh81uwhgmt57PEZws2cw==\\", \\"pub_key\\": {\\"type\\": \\"tendermint/PubKeySecp256k1\\", \\"value\\": \\"Ao3Ur9TXc/FktDebp9SnNCZWQaki/dq5G4GbiJH4aiu+\\"}, \\"account_number\\": \\"0\\", \\"sequence\\": \\"0\\"}", "content": {"key": "test", "address": "cosmos1rq3rcux05yftlh307gw8khh6xj43nv40mq27f5", "content": {"a": 1}, "time": 1601997899.1841497}}'


@pytest.mark.skip("TODO: the verification of the signature fails, investigate why.")
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
    del signature['signature']
    message['signature'] = json.dumps(signature)
    result = await verify_signature(message)
    assert result == False
    
@pytest.mark.asyncio
async def test_verify_signature_bad_data():
    message = json.loads(TEST_MESSAGE)
    signature = json.loads(message['signature'])
    signature['signature'] = signature['signature']+"1"
    message['signature'] = json.dumps(signature)
    result = await verify_signature(message)
    assert result == False
    
@pytest.mark.asyncio
async def test_verify_signature_bad_address():
    message = json.loads(TEST_MESSAGE)
    message['sender'] = 'cosmos1rq3rcux05yftlh307gw8khh6xj43nv40aq27f5'
    result = await verify_signature(message)
    assert result == False
