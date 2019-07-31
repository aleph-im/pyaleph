import pytest
import hashlib
import json
import aleph.chains
from aleph.network import check_message

__author__ = "Moshe Malawach"
__copyright__ = "Moshe Malawach"
__license__ = "mit"


@pytest.mark.asyncio
async def test_check_message_trusted():
    passed_msg = {'foo': 1, 'bar': 2}
    msg = await check_message(passed_msg, trusted=True)
    assert len(msg.keys()) == 3, "same key count plus content_type"
    print(msg)
    assert msg['item_type'] == 'ipfs', "ipfs should be the default"
    assert msg is passed_msg, "same object should be returned"

@pytest.mark.asyncio
async def test_valid_message():
    sample_message = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "2103041b0b357446927d2c8c62fdddd27910d82f665f16a4907a2be927b5901f5e6c004730450221009a54ecaff6869664e94ad68554520c79c21d4f63822864bd910f9916c32c1b5602201576053180d225ec173fb0b6e4af5efb2dc474ce6aa77a3bdd67fd14e1d806b4"
    }
    message = await check_message(sample_message)
    assert message is not None
    
@pytest.mark.asyncio
async def test_invalid_chain_message():
    sample_message = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "chain": "BAR",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "2103041b0b357446927d2c8c62fdddd27910d82f665f16a4907a2be927b5901f5e6c004730450221009a54ecaff6869664e94ad68554520c79c21d4f63822864bd910f9916c32c1b5602201576053180d225ec173fb0b6e4af5efb2dc474ce6aa77a3bdd67fd14e1d806b4"
    }
    message = await check_message(sample_message)
    assert message is None
    
@pytest.mark.asyncio
async def test_invalid_signature_message():
    sample_message = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "time": 1563279102.3155158,
        "signature": "BAR"
    }
    message = await check_message(sample_message)
    assert message is None

@pytest.mark.asyncio
async def test_extraneous_fields():
    sample_message = {
        "item_hash": "QmfDkHXdGND7e8uwJr4yvXSAvbPc8rothM6UN5ABQPsLkF",
        "chain": "NULS",
        "channel": "SYSINFO",
        "sender": "TTanii7eCT93f45g2UpKH81mxpVNcCYw",
        "type": "AGGREGATE",
        "foo": "bar",
        "time": 1563279102.3155158,
        "signature": "2103041b0b357446927d2c8c62fdddd27910d82f665f16a4907a2be927b5901f5e6c004730450221009a54ecaff6869664e94ad68554520c79c21d4f63822864bd910f9916c32c1b5602201576053180d225ec173fb0b6e4af5efb2dc474ce6aa77a3bdd67fd14e1d806b4"
    }
    message = await check_message(sample_message)
    # assert "type" not in message
    assert "foo" not in message

@pytest.mark.asyncio
async def test_inline_content():
    content = json.dumps({'foo': 'bar'})
    h = hashlib.sha256()
    h.update(content.encode('utf-8'))
    sample_message = {
        "item_hash": h.hexdigest(),
        "item_content": content
    }
    message = await check_message(sample_message)

@pytest.mark.asyncio
async def test_signature_fixture_called(mocker):
    print("blah")
    print(mocker)
    raise ValueError("huhu")
