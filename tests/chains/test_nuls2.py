from aleph.chains.nuls2 import Nuls2Connector
from aleph.schemas.pending_messages import parse_message
import pytest


@pytest.mark.asyncio
async def test_verify_signature_nuls2(mocker):
    message_dict = {
        "time": 1574266270.022,
        "type": "POST",
        "chain": "NULS2",
        "sender": "NULSd6HgeZVDvQ2pKQLakAsStYvGAT6WVFu9K",
        "channel": "MYALEPH",
        "content": {
            "ref": "43eef54be4a92c65ca24d3f2419414224129b7944ecaefed088897787aed70b4",
            "time": 1574266270.022,
            "type": "amend",
            "address": "NULSd6HgeZVDvQ2pKQLakAsStYvGAT6WVFu9K",
            "content": {"body": "test", "title": "Mutsi Test", "private": False},
        },
        "item_hash": "43094c3309791a5aa92ff6e1de337f23242103e1dffdc1941c5b6d4131da3a7e",
        "item_type": "inline",
        "signature": "HG4dsFDNGfgjKQX1qorGjxYfK8qEoKF0SfnBSNc8KbpCJ9jET58Rrvc8k3yK8XRl7syoT5gMRmoswOdbSCesmxo=",
        "item_content": '{"type":"amend","address":"NULSd6HgeZVDvQ2pKQLakAsStYvGAT6WVFu9K","content":{"body":"test","title":"Mutsi Test","private":false},"time":1574266270.022,"ref":"43eef54be4a92c65ca24d3f2419414224129b7944ecaefed088897787aed70b4"}',
    }

    connector = Nuls2Connector(
        chain_data_service=mocker.AsyncMock(), session_factory=mocker.Mock()
    )

    message = parse_message(message_dict)
    assert await connector.verify_signature(message)
