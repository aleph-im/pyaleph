import datetime as dt
from typing import List

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models import MessageDb
from aleph.db.models.messages import MessageStatusDb
from aleph.toolkit.timestamp import utc_now
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

ACCOUNT_POST_TYPES_URI = "/api/v0/addresses/{address}/post_types"
ACCOUNT_CHANNELS_URI = "/api/v0/addresses/{address}/channels"
TEST_ADDRESS = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"


@pytest.fixture
def fixture_post_messages_with_types(
    session_factory: DbSessionFactory,
) -> List[MessageDb]:
    """Create POST messages with different post_types for testing."""
    now = utc_now()
    messages = [
        MessageDb(
            item_hash="hash1",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126646.5,"type":"blog","content":{"title":"Post 1"}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126646.5,
                "type": "blog",
                "content": {"title": "Post 1"},
            },
            size=100,
            time=now,
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="hash2",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126647.5,"type":"blog","content":{"title":"Post 2"}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126647.5,
                "type": "blog",
                "content": {"title": "Post 2"},
            },
            size=100,
            time=now + dt.timedelta(seconds=1),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="hash3",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126648.5,"type":"news","content":{"title":"News 1"}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126648.5,
                "type": "news",
                "content": {"title": "News 1"},
            },
            size=100,
            time=now + dt.timedelta(seconds=2),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="hash4",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126649.5,"type":"tutorial","content":{"title":"Tutorial 1"}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126649.5,
                "type": "tutorial",
                "content": {"title": "Tutorial 1"},
            },
            size=100,
            time=now + dt.timedelta(seconds=3),
            channel=Channel("TEST"),
        ),
        # POST message with null type should be ignored in distinct list
        MessageDb(
            item_hash="hash_null_type",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126650.0,"type":null,"content":{"title":"Missing type"}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126650.0,
                "type": None,
                "content": {"title": "Missing type"},
            },
            size=100,
            time=now + dt.timedelta(seconds=3, milliseconds=500),
            channel=Channel("TEST"),
        ),
        # Add a non-POST message to ensure it's filtered out
        MessageDb(
            item_hash="hash5",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","key":"test","time":1652126650.5,"content":{}}',
            content={
                "address": TEST_ADDRESS,
                "key": "test",
                "time": 1652126650.5,
                "content": {},
            },
            size=100,
            time=now + dt.timedelta(seconds=4),
            channel=Channel("TEST"),
        ),
        # Add a POST message from a different address
        MessageDb(
            item_hash="hash6",
            chain=Chain.ETH,
            sender="0xDifferentAddress",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xDifferentAddress","time":1652126651.5,"type":"blog","content":{"title":"Other Post"}}',
            content={
                "address": "0xDifferentAddress",
                "time": 1652126651.5,
                "type": "blog",
                "content": {"title": "Other Post"},
            },
            size=100,
            time=now + dt.timedelta(seconds=5),
            channel=Channel("TEST"),
        ),
    ]

    message_statuses = [
        MessageStatusDb(
            item_hash=msg.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=utc_now(),
        )
        for msg in messages
    ]

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(message_statuses)
        session.commit()

    return messages


@pytest.mark.asyncio
async def test_get_account_post_types(
    ccn_api_client,
    fixture_post_messages_with_types: List[MessageDb],
):
    """Test getting post_types for an address with multiple POST messages."""
    uri = ACCOUNT_POST_TYPES_URI.format(address=TEST_ADDRESS)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == TEST_ADDRESS
    assert "post_types" in data
    # Should return distinct post_types: blog, news, tutorial (sorted)
    assert set(data["post_types"]) == {"blog", "news", "tutorial"}
    assert len(data["post_types"]) == 3
    # Should be sorted
    assert data["post_types"] == sorted(data["post_types"])


@pytest.mark.asyncio
async def test_get_account_post_types_empty(
    ccn_api_client,
):
    """Test getting post_types for an address with no POST messages."""
    uri = ACCOUNT_POST_TYPES_URI.format(address=TEST_ADDRESS)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == TEST_ADDRESS
    assert data["post_types"] == []


@pytest.mark.asyncio
async def test_get_account_post_types_different_address(
    ccn_api_client,
    fixture_post_messages_with_types: List[MessageDb],
):
    """Test getting post_types for a different address."""
    different_address = "0xDifferentAddress"
    uri = ACCOUNT_POST_TYPES_URI.format(address=different_address)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == different_address
    assert "post_types" in data
    assert set(data["post_types"]) == {"blog"}
    assert len(data["post_types"]) == 1


@pytest.mark.asyncio
async def test_get_account_post_types_single_type(
    ccn_api_client,
    session_factory: DbSessionFactory,
):
    """Test getting post_types when address has only one post_type."""
    address = "0xSingleTypeAddress"
    now = utc_now()

    messages = [
        MessageDb(
            item_hash="single1",
            chain=Chain.ETH,
            sender=address,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content=f'{{"address":"{address}","time":1652126646.5,"type":"single","content":{{"title":"Post 1"}}}}',
            content={
                "address": address,
                "time": 1652126646.5,
                "type": "single",
                "content": {"title": "Post 1"},
            },
            size=100,
            time=now,
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="single2",
            chain=Chain.ETH,
            sender=address,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content=f'{{"address":"{address}","time":1652126647.5,"type":"single","content":{{"title":"Post 2"}}}}',
            content={
                "address": address,
                "time": 1652126647.5,
                "type": "single",
                "content": {"title": "Post 2"},
            },
            size=100,
            time=now + dt.timedelta(seconds=1),
            channel=Channel("TEST"),
        ),
    ]

    message_statuses = [
        MessageStatusDb(
            item_hash=msg.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=utc_now(),
        )
        for msg in messages
    ]

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(message_statuses)
        session.commit()

    uri = ACCOUNT_POST_TYPES_URI.format(address=address)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == address
    assert data["post_types"] == ["single"]


@pytest.fixture
def fixture_messages_with_channels(
    session_factory: DbSessionFactory,
) -> List[MessageDb]:
    """Create messages with different channels for testing."""
    now = utc_now()
    messages = [
        MessageDb(
            item_hash="channel_hash1",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126646.5,"type":"blog","content":{}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126646.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=now,
            channel=Channel("channel1"),
        ),
        MessageDb(
            item_hash="channel_hash2",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126647.5,"type":"blog","content":{}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126647.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=now + dt.timedelta(seconds=1),
            channel=Channel("channel1"),
        ),
        MessageDb(
            item_hash="channel_hash3",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","key":"test","time":1652126648.5,"content":{}}',
            content={
                "address": TEST_ADDRESS,
                "key": "test",
                "time": 1652126648.5,
                "content": {},
            },
            size=100,
            time=now + dt.timedelta(seconds=2),
            channel=Channel("channel2"),
        ),
        MessageDb(
            item_hash="channel_hash4",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.store,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126649.5,"item_hash":"hash123","item_type":"ipfs"}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126649.5,
                "item_hash": "hash123",
                "item_type": "ipfs",
            },
            size=100,
            time=now + dt.timedelta(seconds=3),
            channel=Channel("channel3"),
        ),
        # Message with null channel should be filtered out
        MessageDb(
            item_hash="channel_hash5",
            chain=Chain.ETH,
            sender=TEST_ADDRESS,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"'
            + TEST_ADDRESS
            + '","time":1652126650.5,"type":"blog","content":{}}',
            content={
                "address": TEST_ADDRESS,
                "time": 1652126650.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=now + dt.timedelta(seconds=4),
            channel=None,
        ),
        # Message from different address should be filtered out
        MessageDb(
            item_hash="channel_hash6",
            chain=Chain.ETH,
            sender="0xDifferentAddress",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xDifferentAddress","time":1652126651.5,"type":"blog","content":{}}',
            content={
                "address": "0xDifferentAddress",
                "time": 1652126651.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=now + dt.timedelta(seconds=5),
            channel=Channel("other_channel"),
        ),
    ]

    message_statuses = [
        MessageStatusDb(
            item_hash=msg.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=utc_now(),
        )
        for msg in messages
    ]

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(message_statuses)
        session.commit()

    return messages


@pytest.mark.asyncio
async def test_get_account_channels(
    ccn_api_client,
    fixture_messages_with_channels: List[MessageDb],
):
    """Test getting channels for an address with multiple messages."""
    uri = ACCOUNT_CHANNELS_URI.format(address=TEST_ADDRESS)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == TEST_ADDRESS
    assert "channels" in data
    # Should return distinct channels: channel1, channel2, channel3 (sorted)
    assert set(data["channels"]) == {"channel1", "channel2", "channel3"}
    assert len(data["channels"]) == 3
    # Should be sorted
    assert data["channels"] == sorted(data["channels"])


@pytest.mark.asyncio
async def test_get_account_channels_empty(
    ccn_api_client,
):
    """Test getting channels for an address with no messages."""
    uri = ACCOUNT_CHANNELS_URI.format(address=TEST_ADDRESS)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == TEST_ADDRESS
    assert data["channels"] == []


@pytest.mark.asyncio
async def test_get_account_channels_only_null(
    ccn_api_client,
    session_factory: DbSessionFactory,
):
    """Test getting channels when address has only messages with null channels."""
    address = "0xNullChannelsAddress"
    now = utc_now()

    messages = [
        MessageDb(
            item_hash="null1",
            chain=Chain.ETH,
            sender=address,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content=f'{{"address":"{address}","time":1652126646.5,"type":"blog","content":{{}}}}',
            content={
                "address": address,
                "time": 1652126646.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=now,
            channel=None,
        ),
    ]

    message_statuses = [
        MessageStatusDb(
            item_hash=msg.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=utc_now(),
        )
        for msg in messages
    ]

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(message_statuses)
        session.commit()

    uri = ACCOUNT_CHANNELS_URI.format(address=address)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == address
    assert data["channels"] == []


@pytest.mark.asyncio
async def test_get_account_channels_different_address(
    ccn_api_client,
    fixture_messages_with_channels: List[MessageDb],
):
    """Test getting channels for a different address."""
    different_address = "0xDifferentAddress"
    uri = ACCOUNT_CHANNELS_URI.format(address=different_address)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == different_address
    assert "channels" in data
    assert set(data["channels"]) == {"other_channel"}
    assert len(data["channels"]) == 1


@pytest.mark.asyncio
async def test_get_account_channels_single_channel(
    ccn_api_client,
    session_factory: DbSessionFactory,
):
    """Test getting channels when address has only one channel."""
    address = "0xSingleChannelAddress"
    now = utc_now()

    messages = [
        MessageDb(
            item_hash="single1",
            chain=Chain.ETH,
            sender=address,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content=f'{{"address":"{address}","time":1652126646.5,"type":"blog","content":{{}}}}',
            content={
                "address": address,
                "time": 1652126646.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=now,
            channel=Channel("single_channel"),
        ),
        MessageDb(
            item_hash="single2",
            chain=Chain.ETH,
            sender=address,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            item_content=f'{{"address":"{address}","key":"test","time":1652126647.5,"content":{{}}}}',
            content={
                "address": address,
                "key": "test",
                "time": 1652126647.5,
                "content": {},
            },
            size=100,
            time=now + dt.timedelta(seconds=1),
            channel=Channel("single_channel"),
        ),
    ]

    message_statuses = [
        MessageStatusDb(
            item_hash=msg.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=utc_now(),
        )
        for msg in messages
    ]

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(message_statuses)
        session.commit()

    uri = ACCOUNT_CHANNELS_URI.format(address=address)
    response = await ccn_api_client.get(uri)

    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["address"] == address
    assert data["channels"] == ["single_channel"]
