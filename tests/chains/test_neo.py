import pytest
import json

import aleph.chains
from aleph.chains.neo import buildNEOVerification, verify_signature

TEST_MESSAGE = r'{"chain":"NEO","channel":"TEST","sender":"AGBwSUtPi6TiMDBV8mLf7MgSyPyRJL2VUu","type":"POST","time":1584057776.473,"item_type":"inline","item_content":"{\"type\":\"chat\",\"address\":\"AGBwSUtPi6TiMDBV8mLf7MgSyPyRJL2VUu\",\"content\":{\"body\":\"haha\"},\"time\":1584057776.473,\"ref\":\"hall\"}","item_hash":"61b62d8f7dda8764431d70279ddf8f354a6dad4fa574db6e8056b3e14d76e81b","signature":"{\"publicKey\":\"0327054981f0fc922e94a08a76a31f3601ba26b6489fb055498ac0ea4b242de49e\",\"salt\":\"28c0eb4ad85540b6aab2ca08603ce8df\",\"data\":\"aabb3e6facf8fe41faf905082c6ad89bdb5ccfd6a147c59cf63a5a8ccbb8fd739a1a3d7542cede0432ca0676a83ff281993d214855417e002db9a1f00a48bd58\"}"}'

@pytest.mark.asyncio
async def test_buildNEOVerification():
    buffer = await buildNEOVerification({
        'chain': 'CHAIN',
        'sender': 'SENDER',
        'type': 'TYPE',
        'item_hash': 'ITEM_HASH'
    }, 'test')
    assert buffer == '010001f01f74657374434841494e0a53454e4445520a545950450a4954454d5f484153480000'

@pytest.mark.asyncio
async def test_buildNEOVerification_real():
    message = json.loads(TEST_MESSAGE)
    signature = json.loads(message['signature'])
    buffer = await buildNEOVerification(message, signature['salt'])
    assert buffer == '010001f08c32386330656234616438353534306236616162326361303836303363653864664e454f0a4147427753557450693654694d444256386d4c66374d6753795079524a4c325655750a504f53540a363162363264386637646461383736343433316437303237396464663866333534613664616434666135373464623665383035366233653134643736653831620000'


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
async def test_verify_signature_no_salt():
    message = json.loads(TEST_MESSAGE)
    signature = json.loads(message['signature'])
    del signature['salt']
    message['signature'] = json.dumps(signature)
    result = await verify_signature(message)
    assert result == False
    
@pytest.mark.asyncio
async def test_verify_signature_no_pubkey():
    message = json.loads(TEST_MESSAGE)
    signature = json.loads(message['signature'])
    del signature['publicKey']
    message['signature'] = json.dumps(signature)
    result = await verify_signature(message)
    assert result == False
    
@pytest.mark.asyncio
async def test_verify_signature_no_data():
    message = json.loads(TEST_MESSAGE)
    signature = json.loads(message['signature'])
    del signature['data']
    message['signature'] = json.dumps(signature)
    result = await verify_signature(message)
    assert result == False