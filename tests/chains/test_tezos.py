import pytest
from aleph_message.models import Chain

from aleph.chains.tezos import (
    TezosConnector,
    datetime_to_iso_8601,
    indexer_event_to_chain_tx,
)
from aleph.db.models import PendingMessageDb
from aleph.schemas.chains.tezos_indexer_response import (
    IndexerMessageEvent,
    MessageEventPayload,
)
from aleph.schemas.pending_messages import parse_message
import datetime as dt

from aleph.types.chain_sync import ChainSyncProtocol


@pytest.mark.asyncio
async def test_tezos_verify_signature_raw(mocker):
    message_dict = {
        "chain": "TEZOS",
        "channel": "TEST",
        "sender": "tz2M3NQJ982QV1YwvGL77drZCY55tfBzWm3x",
        "type": "POST",
        "time": 1657534863.1375434,
        "item_content": '{"address":"tz2M3NQJ982QV1YwvGL77drZCY55tfBzWm3x","time":1657534863.1371658,"content":{"status":"testing"},"type":"test"}',
        "item_hash": "75bce3f296c6479242c740a051c2ddef4184f39386a883ceb33bf1f36f45ad09",
        "signature": '{"publicKey": "sppk7cKmJSNo8LxB6R3eUGGRg3Lt7xn6K4wRNNxjSLxeB5zPZhvkQ6k", "signature": "spsig1Z3B4PduY14W2FjbHPuaYG8CRSFzJiZeNDPhdivyiDGqyAG1ZRKtubTmq7zfKywekzUBaAdop6mxoodBfq8Yi9RqroECk5"}',
        "content": {
            "address": "tz2M3NQJ982QV1YwvGL77drZCY55tfBzWm3x",
            "time": 1657534863.1371658,
            "content": {"status": "testing"},
            "type": "test",
        },
    }
    connector = TezosConnector(
        session_factory=mocker.MagicMock(), chain_data_service=mocker.AsyncMock()
    )

    message = parse_message(message_dict)
    assert await connector.verify_signature(message)


@pytest.mark.asyncio
async def test_tezos_verify_signature_raw_ed25519(mocker):
    message_dict = {
        "chain": "TEZOS",
        "sender": "tz1SmGHzna3YhKropa3WudVq72jhTPDBn4r5",
        "type": "POST",
        "channel": "ALEPH-TEST",
        "signature": '{"signature":"siggLSTX5i9ZZJHb6vUoi5gNxWEjEcBD62Jjs8JdFgDND3uc9xb5YC9bUFLpBAoudhdTRNfmV7GTnJzoWUm9y1cDh7T6KX59","publicKey":"edpkvUuhtQDPA9KfC3BY7ydh89hT34KTANMfX7L22BUrA9aGWg6QxF"}',
        "time": 1661451074.86,
        "item_type": "inline",
        "item_content": '{"type":"custom_type","address":"tz1SmGHzna3YhKropa3WudVq72jhTPDBn4r5","content":{"body":"Hello World TEZOS"},"time":1661451074.86}',
        "item_hash": "41de1a7766c7e5fad54772470eefde63b6bef8683c4159d9179d74955009deb4",
    }

    connector = TezosConnector(
        session_factory=mocker.MagicMock(), chain_data_service=mocker.AsyncMock()
    )

    message = parse_message(message_dict)
    assert await connector.verify_signature(message)


@pytest.mark.asyncio
async def test_tezos_verify_signature_micheline(mocker):
    message_dict = {
        "chain": "TEZOS",
        "sender": "tz1VrPqrVdMFsgykWyhGH7SYcQ9avHTjPcdD",
        "type": "POST",
        "channel": "ALEPH-TEST",
        "signature": '{"signingType":"micheline","signature":"sigXD8iT5ivdawgPzE1AbtDwqqAjJhS5sHS1psyE74YjfiaQnxWZsATNjncdsuQw3b9xaK79krxtsC8uQoT5TcUXmo66aovT","publicKey":"edpkvapDnjnasrNcmUdMZXhQZwpX6viPyuGCq6nrP4W7ZJCm7EFTpS"}',
        "time": 1663944079.029,
        "item_type": "storage",
        "item_hash": "72b2722b95582419cfa71f631ff6c6afc56344dc6a4609e772877621813040b7",
    }
    connector = TezosConnector(
        session_factory=mocker.MagicMock(), chain_data_service=mocker.AsyncMock()
    )

    message = PendingMessageDb.from_message_dict(
        message_dict,
        reception_time=dt.datetime(2022, 1, 1),
        fetched=True,
    )
    assert await connector.verify_signature(message)


def test_datetime_to_iso_8601():
    naive_datetime = dt.datetime(2022, 1, 1, 12, 6, 23, 675789)
    datetime_str = datetime_to_iso_8601(naive_datetime)

    assert datetime_str == "2022-01-01T12:06:23.675Z"


def test_indexer_event_to_aleph_message():
    indexer_event = IndexerMessageEvent(
        source="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        timestamp="2022-11-16T00:00:00Z",
        type="MessageEvent",
        operationHash="oorMNgusX6RxZ4NhzYriVDN8HDeMBNkjD3E8kx9a7j7dRRDGkzz",
        blockLevel=584664,
        payload=MessageEventPayload(
            timestamp=1668611900,
            addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
            msgtype="STORE_IPFS",
            msgcontent="QmaMLRsvmDRCezZe2iebcKWtEzKNjBaQfwcu7mcpdm8eY2",
        ),
    )

    tx = indexer_event_to_chain_tx(indexer_event)

    assert tx.chain == Chain.TEZOS
    assert tx.datetime == indexer_event.timestamp
    assert tx.publisher == indexer_event.source
    assert tx.hash == indexer_event.operation_hash
    assert tx.height == indexer_event.block_level

    assert tx.protocol == ChainSyncProtocol.SMART_CONTRACT
    assert tx.protocol_version == 1
    assert tx.content == indexer_event.payload.dict()
