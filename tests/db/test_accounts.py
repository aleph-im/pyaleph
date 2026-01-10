import datetime as dt
from typing import List

import pytest
import pytz
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.messages import (
    get_distinct_channels_for_address,
    get_distinct_post_types_for_address,
    get_message_stats_by_address,
    refresh_address_stats_mat_view,
)
from aleph.db.models import MessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory


@pytest.fixture
def fixture_messages():
    return [
        MessageDb(
            item_hash="e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5",
            chain=Chain.ETH,
            sender="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            signature="0xabfa661aab1a9f58955940ea213387de4773f8b1f244c2236cd4ac5ba7bf2ba902e17074bc4b289ba200807bb40951f4249668b055dc15af145b8842ecfad0601c",
            item_type=ItemType.storage,
            type=MessageType.forget,
            item_content=None,
            content={
                "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "time": 1645794065.439,
                "aggregates": [],
                "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
                "reason": "None",
            },
            size=154,
            time=timestamp_to_datetime(1645794065.439),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4",
            chain=Chain.ETH,
            sender="0x1234",
            signature="0x705ca1365a0b794cbfcf89ce13239376d0aab0674d8e7f39965590a46e5206a664bc4b313f3351f313564e033c9fe44fd258492dfbd6c36b089677d73224da0a1c",
            type=MessageType.aggregate,
            item_content='{"address": "0x51A58800b26AA1451aaA803d1746687cB88E0500", "key": "my-aggregate", "time": 1664999873, "content": {"easy": "as", "a-b": "c"}}',
            content={
                "address": "0x1234",
                "key": "my-aggregate",
                "time": 1664999873,
                "content": {"easy": "as", "a-b": "c"},
            },
            item_type=ItemType.inline,
            size=2000,
            time=pytz.utc.localize(
                dt.datetime.fromtimestamp(1664999872, dt.timezone.utc)
            ),
            channel=Channel("CHANEL-N5"),
        ),
    ]


@pytest.mark.asyncio
async def test_get_message_stats_by_address(
    session_factory: DbSessionFactory, fixture_messages: List[MessageDb]
):
    # No data test
    with session_factory() as session:
        stats_no_data = get_message_stats_by_address(session)
        assert stats_no_data == []

        # Refresh the materialized view
        session.add_all(fixture_messages)
        session.commit()

        refresh_address_stats_mat_view(session)
        session.commit()

        stats = get_message_stats_by_address(session)
        assert len(stats) == 2

        stats_by_address = {row.address: row for row in stats}
        assert (
            stats_by_address["0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"].forget == 1
        )
        assert stats_by_address["0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"].total == 1
        assert stats_by_address["0x1234"].aggregate == 1
        assert stats_by_address["0x1234"].total == 1

        # Filter by address
        stats = get_message_stats_by_address(session, addresses=("0x1234",))
        assert len(stats) == 1
        row = stats[0]
        assert row.address == "0x1234"
        assert row.aggregate == 1
        assert row.total == 1


@pytest.fixture
def fixture_post_messages_for_types():
    """Create POST messages with different post_types for testing."""
    return [
        MessageDb(
            item_hash="post_hash1",
            chain=Chain.ETH,
            sender="0xPostAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xPostAddress123","time":1652126646.5,"type":"blog","content":{}}',
            content={
                "address": "0xPostAddress123",
                "time": 1652126646.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126646.5),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="post_hash2",
            chain=Chain.ETH,
            sender="0xPostAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xPostAddress123","time":1652126647.5,"type":"blog","content":{}}',
            content={
                "address": "0xPostAddress123",
                "time": 1652126647.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126647.5),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="post_hash3",
            chain=Chain.ETH,
            sender="0xPostAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xPostAddress123","time":1652126648.5,"type":"news","content":{}}',
            content={
                "address": "0xPostAddress123",
                "time": 1652126648.5,
                "type": "news",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126648.5),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="post_hash4",
            chain=Chain.ETH,
            sender="0xPostAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xPostAddress123","time":1652126649.5,"type":"tutorial","content":{}}',
            content={
                "address": "0xPostAddress123",
                "time": 1652126649.5,
                "type": "tutorial",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126649.5),
            channel=Channel("TEST"),
        ),
        # POST message with null type should be ignored
        MessageDb(
            item_hash="post_hash_null_type",
            chain=Chain.ETH,
            sender="0xPostAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xPostAddress123","time":1652126650.0,"type":null,"content":{}}',
            content={
                "address": "0xPostAddress123",
                "time": 1652126650.0,
                "type": None,
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126650.0),
            channel=Channel("TEST"),
        ),
        # Non-POST message should be filtered out
        MessageDb(
            item_hash="agg_hash1",
            chain=Chain.ETH,
            sender="0xPostAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            item_content='{"address":"0xPostAddress123","key":"test","time":1652126650.5,"content":{}}',
            content={
                "address": "0xPostAddress123",
                "key": "test",
                "time": 1652126650.5,
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126650.5),
            channel=Channel("TEST"),
        ),
        # POST message from different address should be filtered out
        MessageDb(
            item_hash="post_hash5",
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
            time=timestamp_to_datetime(1652126651.5),
            channel=Channel("TEST"),
        ),
    ]


@pytest.mark.asyncio
async def test_get_distinct_post_types_for_address(
    session_factory: DbSessionFactory,
    fixture_post_messages_for_types: List[MessageDb],
):
    """Test getting distinct post_types for an address."""
    address = "0xPostAddress123"

    # No data test
    with session_factory() as session:
        post_types = get_distinct_post_types_for_address(session, address)
        assert post_types == []

        # Add messages
        session.add_all(fixture_post_messages_for_types)
        session.commit()

        # Get distinct post_types
        post_types = get_distinct_post_types_for_address(session, address)

        # Should return distinct post_types: blog, news, tutorial (sorted)
        assert set(post_types) == {"blog", "news", "tutorial"}
        assert len(post_types) == 3
        # Should be sorted
        assert post_types == sorted(post_types)

        # Test with different address
        different_address = "0xDifferentAddress"
        post_types = get_distinct_post_types_for_address(session, different_address)
        assert post_types == ["blog"]

        # Test with address that has no POST messages
        empty_address = "0xEmptyAddress"
        post_types = get_distinct_post_types_for_address(session, empty_address)
        assert post_types == []


@pytest.fixture
def fixture_messages_for_channels():
    """Create messages with different channels for testing."""
    return [
        MessageDb(
            item_hash="channel_hash1",
            chain=Chain.ETH,
            sender="0xChannelAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xChannelAddress123","time":1652126646.5,"type":"blog","content":{}}',
            content={
                "address": "0xChannelAddress123",
                "time": 1652126646.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126646.5),
            channel=Channel("channel1"),
        ),
        MessageDb(
            item_hash="channel_hash2",
            chain=Chain.ETH,
            sender="0xChannelAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xChannelAddress123","time":1652126647.5,"type":"blog","content":{}}',
            content={
                "address": "0xChannelAddress123",
                "time": 1652126647.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126647.5),
            channel=Channel("channel1"),
        ),
        MessageDb(
            item_hash="channel_hash3",
            chain=Chain.ETH,
            sender="0xChannelAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            item_content='{"address":"0xChannelAddress123","key":"test","time":1652126648.5,"content":{}}',
            content={
                "address": "0xChannelAddress123",
                "key": "test",
                "time": 1652126648.5,
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126648.5),
            channel=Channel("channel2"),
        ),
        MessageDb(
            item_hash="channel_hash4",
            chain=Chain.ETH,
            sender="0xChannelAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.store,
            item_content='{"address":"0xChannelAddress123","time":1652126649.5,"item_hash":"hash123","item_type":"ipfs"}',
            content={
                "address": "0xChannelAddress123",
                "time": 1652126649.5,
                "item_hash": "hash123",
                "item_type": "ipfs",
            },
            size=100,
            time=timestamp_to_datetime(1652126649.5),
            channel=Channel("channel3"),
        ),
        # Message with null channel should be filtered out
        MessageDb(
            item_hash="channel_hash5",
            chain=Chain.ETH,
            sender="0xChannelAddress123",
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content='{"address":"0xChannelAddress123","time":1652126650.5,"type":"blog","content":{}}',
            content={
                "address": "0xChannelAddress123",
                "time": 1652126650.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126650.5),
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
            time=timestamp_to_datetime(1652126651.5),
            channel=Channel("other_channel"),
        ),
    ]


@pytest.mark.asyncio
async def test_get_distinct_channels_for_address(
    session_factory: DbSessionFactory,
    fixture_messages_for_channels: List[MessageDb],
):
    """Test getting distinct channels for an address."""
    address = "0xChannelAddress123"

    # No data test
    with session_factory() as session:
        channels = get_distinct_channels_for_address(session, address)
        assert channels == []

        # Add messages
        session.add_all(fixture_messages_for_channels)
        session.commit()

        # Get distinct channels
        channels = get_distinct_channels_for_address(session, address)

        # Should return distinct channels: channel1, channel2, channel3 (sorted)
        assert set(channels) == {"channel1", "channel2", "channel3"}
        assert len(channels) == 3
        # Should be sorted
        assert channels == sorted(channels)

        # Test with different address
        different_address = "0xDifferentAddress"
        channels = get_distinct_channels_for_address(session, different_address)
        assert channels == ["other_channel"]

        # Test with address that has no messages
        empty_address = "0xEmptyAddress"
        channels = get_distinct_channels_for_address(session, empty_address)
        assert channels == []

        # Test with address that has only null channels
        null_channel_address = "0xNullChannelAddress"
        null_message = MessageDb(
            item_hash="null_hash",
            chain=Chain.ETH,
            sender=null_channel_address,
            signature="0x" + "0" * 128,
            item_type=ItemType.inline,
            type=MessageType.post,
            item_content=f'{{"address":"{null_channel_address}","time":1652126652.5,"type":"blog","content":{{}}}}',
            content={
                "address": null_channel_address,
                "time": 1652126652.5,
                "type": "blog",
                "content": {},
            },
            size=100,
            time=timestamp_to_datetime(1652126652.5),
            channel=None,
        )
        session.add(null_message)
        session.commit()

        channels = get_distinct_channels_for_address(session, null_channel_address)
        assert channels == []
