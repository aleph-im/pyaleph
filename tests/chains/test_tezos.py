import pytest
from aleph_message.models import (
    Chain,
    MessageType,
    ItemType,
    StoreContent,
    PostContent,
    AggregateContent,
)

from aleph.chains.tezos import indexer_event_to_aleph_message
from aleph.exceptions import InvalidMessageError
from aleph.network import verify_signature
from aleph.schemas.chains.tezos_indexer_response import (
    IndexerMessageEvent,
    MessageEventPayload,
)
from aleph.schemas.pending_messages import parse_message


@pytest.mark.asyncio
async def test_tezos_verify_signature():
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

    message = parse_message(message_dict)
    await verify_signature(message)


@pytest.mark.asyncio
async def test_tezos_verify_signature_ed25519():
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

    message = parse_message(message_dict)
    await verify_signature(message)


def test_indexer_event_to_aleph_message_store_ipfs():
    indexer_event = IndexerMessageEvent(
        source="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        timestamp="2022-11-16T00:00:00Z",
        type="MessageEvent",
        blockHash="BMaSNJJCebD52e37nNEmSntx9UraJo2js5QT9siXA34E7a8gzRc",
        blockLevel=584664,
        payload=MessageEventPayload(
            timestamp=1668611900,
            addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
            msgtype="STORE_IPFS",
            msgcontent="QmaMLRsvmDRCezZe2iebcKWtEzKNjBaQfwcu7mcpdm8eY2",
        ),
    )

    pending_message, tx_context = indexer_event_to_aleph_message(indexer_event)

    assert (
        pending_message.item_hash
        == "c83c515d48a8df8538f3a13eb2ee31b30b8f80c820ef2771c34e4b0b9e97e00f"
    )
    assert pending_message.sender == indexer_event.payload.addr
    assert pending_message.chain == Chain.TEZOS
    assert pending_message.signature is None
    assert pending_message.type == MessageType.store
    assert pending_message.item_type == ItemType.inline
    assert pending_message.channel is None

    message_content = StoreContent.parse_raw(pending_message.item_content)
    assert message_content.item_hash == indexer_event.payload.message_content
    assert message_content.item_type == ItemType.ipfs
    assert message_content.address == indexer_event.payload.addr
    assert message_content.time == indexer_event.payload.timestamp

    assert tx_context.chain_name == Chain.TEZOS
    assert tx_context.time == indexer_event.timestamp.timestamp()
    assert tx_context.publisher == indexer_event.source
    assert tx_context.tx_hash == indexer_event.block_hash
    assert tx_context.height == indexer_event.block_level


def test_indexer_event_to_aleph_message_post():
    content = PostContent(
        content={"body": "My first post on Tezos"},
        ref=None,
        type="my-type",
        address="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        time=1000,
    )

    indexer_event = IndexerMessageEvent(
        source="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        timestamp="2022-11-16T00:00:00Z",
        type="MessageEvent",
        blockHash="BMaSNJJCebD52e37nNEmSntx9UraJo2js5QT9siXA34E7a8gzRc",
        blockLevel=584664,
        payload=MessageEventPayload(
            timestamp=1668611900,
            addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
            msgtype="POST",
            msgcontent=content.json(),
        ),
    )

    pending_message, tx_context = indexer_event_to_aleph_message(indexer_event)

    assert (
        pending_message.item_hash
        == "cbe9c48c7290d6e243c80247444c6d28c36a475c99286b6e921b5223dc2cba39"
    )
    assert pending_message.sender == indexer_event.payload.addr
    assert pending_message.chain == Chain.TEZOS
    assert pending_message.signature is None
    assert pending_message.type == MessageType.post
    assert pending_message.item_type == ItemType.inline
    assert pending_message.channel is None

    message_content = PostContent.parse_raw(pending_message.item_content)
    assert message_content.address == content.address
    assert message_content.time == content.time
    assert message_content.ref == content.ref
    assert message_content.type == content.type
    assert message_content.content == content.content

    assert tx_context.chain_name == Chain.TEZOS
    assert tx_context.time == indexer_event.timestamp.timestamp()
    assert tx_context.publisher == indexer_event.source
    assert tx_context.tx_hash == indexer_event.block_hash
    assert tx_context.height == indexer_event.block_level


def test_indexer_event_to_aleph_message_aggregate():
    content = AggregateContent(
        key="my-aggregate",
        content={"body": "My first post on Tezos"},
        address="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        time=1000,
    )

    indexer_event = IndexerMessageEvent(
        source="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        timestamp="2022-11-16T00:00:00Z",
        type="MessageEvent",
        blockHash="BMaSNJJCebD52e37nNEmSntx9UraJo2js5QT9siXA34E7a8gzRc",
        blockLevel=584664,
        payload=MessageEventPayload(
            timestamp=1668611900,
            addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
            msgtype="AGGREGATE",
            msgcontent=content.json(),
        ),
    )

    pending_message, tx_context = indexer_event_to_aleph_message(indexer_event)

    assert (
        pending_message.item_hash
        == "8771f3be34fdb380f98bb9e4e6b37658c68581317e3d5efed966e53caa060a5b"
    )
    assert pending_message.sender == indexer_event.payload.addr
    assert pending_message.chain == Chain.TEZOS
    assert pending_message.signature is None
    assert pending_message.type == MessageType.aggregate
    assert pending_message.item_type == ItemType.inline
    assert pending_message.channel is None

    message_content = AggregateContent.parse_raw(pending_message.item_content)
    assert message_content.address == content.address
    assert message_content.time == content.time
    assert message_content.key == content.key
    assert message_content.content == content.content

    assert tx_context.chain_name == Chain.TEZOS
    assert tx_context.time == indexer_event.timestamp.timestamp()
    assert tx_context.publisher == indexer_event.source
    assert tx_context.tx_hash == indexer_event.block_hash
    assert tx_context.height == indexer_event.block_level


def test_indexer_event_to_aleph_message_invalid_payload():
    indexer_event = IndexerMessageEvent(
        source="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        timestamp="2022-11-16T00:00:00Z",
        type="MessageEvent",
        blockHash="BMaSNJJCebD52e37nNEmSntx9UraJo2js5QT9siXA34E7a8gzRc",
        blockLevel=584664,
        payload=MessageEventPayload(
            timestamp=1668611900,
            addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
            msgtype="POST",
            msgcontent="",
        ),
    )

    # Invalid JSON (empty message content)
    with pytest.raises(InvalidMessageError):
        _ = indexer_event_to_aleph_message(indexer_event)

    # Wrong content type, aggregate for a post
    invalid_content = AggregateContent(
        key="my-aggregate",
        content={"body": "My first post on Tezos"},
        address="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        time=1000,
    )
    indexer_event.payload.message_content = invalid_content.json()

    with pytest.raises(InvalidMessageError):
        _ = indexer_event_to_aleph_message(indexer_event)

    # Invalid message type
    indexer_event.payload.message_type = "NOPE"

    with pytest.raises(InvalidMessageError):
        _ = indexer_event_to_aleph_message(indexer_event)
