import pytest

import aleph.chains
from aleph.chains.common import get_verification_buffer, mark_confirmed_data


@pytest.mark.asyncio
async def test_get_verification_buffer():
    buffer = await get_verification_buffer({
        'chain': 'CHAIN',
        'sender': 'SENDER',
        'type': 'TYPE',
        'item_hash': 'ITEM_HASH'
    })
    assert buffer == b'CHAIN\nSENDER\nTYPE\nITEM_HASH'


@pytest.mark.asyncio
async def test_mark_confirmed_data():
    value = await mark_confirmed_data('CHAIN', 'TXHASH', 99999999)
    assert value['confirmed'] is True
    assert len(value['confirmations']) == 1
    assert value['confirmations'][0]['chain'] == 'CHAIN'
    assert value['confirmations'][0]['height'] == 99999999
    assert value['confirmations'][0]['hash'] == 'TXHASH'
