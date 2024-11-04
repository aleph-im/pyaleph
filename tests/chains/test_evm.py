import pytest

from aleph.chains.evm import EVMVerifier
from aleph.schemas.pending_messages import BasePendingMessage, parse_message


@pytest.fixture
def evm_message() -> BasePendingMessage:
    return parse_message(
        {
            "item_hash": "f524a258d87f1771e8538fd4fd91acdcc527c3b7f138fafd6ff89a5fcf97c3b7",
            "type": "POST",
            "chain": "ETH",
            "sender": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "signature": "0x99efc66c781c889e1f21c680869c832141dcee90189e75e85f570b8b49e72dee0338d77c214ae55bfcb886bbd7bac6dc4dcfda4eb0d2c47ed93d51b36b7259b01c",
            "time": 1730410918.092607,
            "item_type": "inline",
            "item_content": '{"address":"0xA07B1214bAe0D5ccAA25449C3149c0aC83658874","time":1730410918.0924816,"content":{"type":"polygon","address":"0xA07B1214bAe0D5ccAA25449C3149c0aC83658874","content":{"body":"This message was posted from the typescript-SDK test suite"},"time":1689163528.372},"type":"test"}',
            "channel": "ALEPH-CLOUDSOLUTIONS",
        }
    )


@pytest.mark.asyncio
async def test_verify_evm_signature_real(evm_message: BasePendingMessage):
    verifier = EVMVerifier()
    result = await verifier.verify_signature(evm_message)
    assert result is True


@pytest.mark.asyncio
async def test_verify_bad_evm_signature(evm_message: BasePendingMessage):
    verifier = EVMVerifier()
    evm_message.signature = "baba"
    result = await verifier.verify_signature(evm_message)
    assert result is False
