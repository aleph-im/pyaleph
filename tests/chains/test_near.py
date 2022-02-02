import pytest
import json

import aleph.chains
from aleph.chains.near import verify_signature

TEST_MESSAGE = '{"chain": "NEAR", "channel": "TEST", "sender": "thewinnie.testnet:7KRctNKvQbDFxT6QhvUHgznCqEJtQvQohzjsZ9fPrjaC", "type": "AGGREGATE", "time": 1642441245.147, "item_content": "{\\"key\\":\\"neeaarr\\",\\"address\\":\\"thewinnie.testnet:7KRctNKvQbDFxT6QhvUHgznCqEJtQvQohzjsZ9fPrjaC\\",\\"content\\":{\\"A\\":1},\\"time\\":1642441245.1478}", "item_hash": "9ef40466c005e7c8161804583ac0395e6274366ce3f839315a5ce9a4dd521528", "signature": "{\\"publicKey\\": \\"7KRctNKvQbDFxT6QhvUHgznCqEJtQvQohzjsZ9fPrjaC\\", \\"signature\\": \\"2qnE7eaE7i75K7Tic3NAMT48XHCnkP2HSRW22Kb6KiWT3uWaUaH6jHVHgfocUhKyyWBVbAtes8cXTDdp3zm5c466\\"}", "content": {"key": "neeaarr", "address": "thewinnie.testnet:7KRctNKvQbDFxT6QhvUHgznCqEJtQvQohzjsZ9fPrjaC", "content": {"A": 1}, "time": 1642441245.147}}'

@pytest.mark.asyncio
async def test_verify_signature_real():
    message = json.loads(TEST_MESSAGE)
    result = await verify_signature(message)
    assert result is True

@pytest.mark.asyncio
async def test_verify_signature_nonexistent():
    result = await verify_signature({
        'chain': 'CHAIN',
        'sender': 'SENDER',
        'type': 'TYPE',
        'item_hash': 'ITEM_HASH'
    })
    assert result is False

@pytest.mark.asyncio
async def test_verify_signature_wrong_format():
    result = await verify_signature({
        'chain': 'CHAIN',
        'sender': 'SENDER',
        'type': 'TYPE',
        'item_hash': 'ITEM_HASH',
        'signature': 'fakeSignature'
    })
    assert result is False
