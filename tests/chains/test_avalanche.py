import pytest

from aleph.chains.avalanche import AvalancheConnector
from aleph.schemas.pending_messages import parse_message, BasePendingMessage


@pytest.fixture
def avax_message() -> BasePendingMessage:
    return parse_message(
        {
            "item_hash": "3c4d948a22c3d41b7d189555ee4a285cb490ec553f3d135cc3b2f0cfddf5c0f2",
            "type": "POST",
            "chain": "AVAX",
            "sender": "X-avax14x5a42stua94l2vxjcag6c9ftd8ea0y8fltdwv",
            "signature": "3WRUvPbp7euNQvxuhV2YaFUJHN2Xoo8yku67MTuhfk8bRvDQz6hysQrrkfyKweXSCDNzfjrYzd1PwhGWdTJGZAvuMPiEJvJ",
            "item_type": "inline",
            "item_content": '{"type":"avalanche","address":"X-avax14x5a42stua94l2vxjcag6c9ftd8ea0y8fltdwv","content":{"body":"This message was posted from the typescript-SDK test suite"},"time":1689163528.372}',
            "time": 1689163528.372,
            "channel": "TEST",
        }
    )


@pytest.mark.asyncio
async def test_verify_signature_real(avax_message: BasePendingMessage):
    connector = AvalancheConnector()
    result = await connector.verify_signature(avax_message)
    assert result is True


@pytest.mark.asyncio
async def test_verify_signature_bad_base58(avax_message: BasePendingMessage):
    connector = AvalancheConnector()
    avax_message.signature = "baba"
    result = await connector.verify_signature(avax_message)
    assert result is False
