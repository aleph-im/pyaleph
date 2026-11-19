from types import SimpleNamespace

import pytest
from configmanager import Config

from aleph.chains.nuls2 import Nuls2Connector, Nuls2Verifier
from aleph.schemas.pending_messages import parse_message


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

    verifier = Nuls2Verifier()
    message = parse_message(message_dict)
    assert await verifier.verify_signature(message)


@pytest.mark.asyncio
async def test_anchor_unconfirmed_messages_anchors_every_batch(
    mock_config: Config, mocker
):
    mock_config.aleph.jobs.max_unconfirmed_messages.value = 2

    batches = [
        [
            SimpleNamespace(item_hash="hash-1", reception_time=1),
            SimpleNamespace(item_hash="hash-2", reception_time=1),
        ],
        [
            SimpleNamespace(item_hash="hash-3", reception_time=2),
            SimpleNamespace(item_hash="hash-4", reception_time=2),
        ],
        [SimpleNamespace(item_hash="hash-5", reception_time=3)],
        [],
    ]
    get_unconfirmed_messages = mocker.patch(
        "aleph.chains.nuls2.get_unconfirmed_messages", side_effect=batches
    )

    tx = mocker.MagicMock()
    tx.sign_tx = mocker.AsyncMock()
    tx.serialize = mocker.AsyncMock(return_value=b"tx")
    prepare_transfer_tx = mocker.patch(
        "aleph.chains.nuls2.prepare_transfer_tx",
        new_callable=mocker.AsyncMock,
        return_value=tx,
    )
    broadcast = mocker.patch(
        "aleph.chains.nuls2.broadcast",
        new_callable=mocker.AsyncMock,
        side_effect=[
            {"hash": "1" * 64},
            {"hash": "2" * 64},
            {"hash": "3" * 64},
        ],
    )

    connector = object.__new__(Nuls2Connector)
    connector.session_factory = mocker.MagicMock()
    connector.chain_data_service = mocker.MagicMock()
    connector.chain_data_service.prepare_sync_event_payload = mocker.AsyncMock(
        return_value=SimpleNamespace(json=lambda: "payload")
    )

    anchored, nonce = await connector._anchor_unconfirmed_messages(
        config=mock_config,
        server=None,
        address="address",
        target_addr="target",
        remark=b"remark",
        pri_key=b"key",
        chain_id=1,
        nonce="0" * 16,
    )

    assert anchored == 5
    assert nonce == "3" * 16
    assert broadcast.await_count == 3
    assert [call.args[2] for call in prepare_transfer_tx.call_args_list] == [
        "0" * 16,
        "1" * 16,
        "2" * 16,
    ]

    calls = get_unconfirmed_messages.call_args_list
    assert calls[0].kwargs["after"] is None
    assert calls[1].kwargs["after"] == (1, "hash-2")
    assert calls[2].kwargs["after"] == (2, "hash-4")
    assert calls[3].kwargs["after"] == (3, "hash-5")
