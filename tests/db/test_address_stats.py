"""
Tests for address stats functions.
"""

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.messages import (
    fetch_stats_for_addresses,
    find_matching_addresses,
    refresh_address_stats_mat_view,
)
from aleph.db.models import MessageDb
from aleph.schemas.addresses_query_params import SortBy
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrder


def create_test_messages():
    """
    Creates test messages with different addresses for testing address stats functions.
    """
    return [
        MessageDb(
            item_hash="test_hash1",
            chain=Chain.ETH,
            sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            signature="0xsig1",
            item_type=ItemType.inline,
            type=MessageType.post,
            content={"test": "content1"},
            size=100,
            time=timestamp_to_datetime(1645794065.101),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="test_hash2",
            chain=Chain.ETH,
            sender="0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",
            signature="0xsig2",
            item_type=ItemType.inline,
            type=MessageType.post,
            content={"test": "content2"},
            size=100,
            time=timestamp_to_datetime(1645794065.102),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="test_hash3",
            chain=Chain.ETH,
            sender="0x5D00fAD0763A876202a29FE71D30B4554D28FB97",
            signature="0xsig3",
            item_type=ItemType.inline,
            type=MessageType.store,
            content={"test": "content3"},
            size=100,
            time=timestamp_to_datetime(1645794065.103),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="test_hash4",
            chain=Chain.ETH,
            sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            signature="0xsig4",
            item_type=ItemType.inline,
            type=MessageType.store,
            content={"test": "content4"},
            size=100,
            time=timestamp_to_datetime(1645794065.104),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="test_hash5",
            chain=Chain.ETH,
            sender="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            signature="0xsig5",
            item_type=ItemType.inline,
            type=MessageType.program,
            content={"test": "content5"},
            size=100,
            time=timestamp_to_datetime(1645794065.105),
            channel=Channel("TEST"),
        ),
    ]


@pytest.mark.asyncio
async def test_find_matching_addresses(session_factory: DbSessionFactory):
    """Test the function that searches for addresses containing a substring."""
    with session_factory() as session:
        test_messages = create_test_messages()
        session.add_all(test_messages)
        session.commit()

        # Refresh views to ensure we have updated stats
        refresh_address_stats_mat_view(session)
        session.commit()

        # Test search with uppercase pattern - should be case insensitive
        addresses = find_matching_addresses(session, address_contains="0X69")
        assert (
            len(addresses) >= 1
        )  # Should find 0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106

        # Test search with lowercase pattern
        addresses = find_matching_addresses(session, address_contains="ac033")
        assert (
            len(addresses) >= 1
        )  # Should find 0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4

        # Test search with pattern that should match all addresses
        addresses = find_matching_addresses(session, address_contains="")
        assert len(addresses) >= 3

        # Test search with no matching addresses
        addresses = find_matching_addresses(session, address_contains="NOMATCH")
        assert len(addresses) == 0

        # Test search with limit
        addresses = find_matching_addresses(session, address_contains="", limit=1)
        assert len(addresses) == 1

        # Test with exact address
        addresses = find_matching_addresses(
            session,
            address_contains="0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
            limit=1,
        )
        assert len(addresses) == 1


@pytest.mark.asyncio
async def test_fetch_stats_for_addresses(session_factory: DbSessionFactory):
    """Test the fetch_stats_for_addresses function with various sorting and filtering options."""
    with session_factory() as session:
        test_messages = create_test_messages()
        session.add_all(test_messages)
        session.commit()

        # Refresh materialized views to ensure we have updated stats
        refresh_address_stats_mat_view(session)
        session.commit()

        # Test with default parameters (sort by total messages, descending)
        stats, total_count = fetch_stats_for_addresses(
            session=session,
            addresses=None,
            sort_by=SortBy.MESSAGES,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=20,
        )

        # Check stats structure
        assert isinstance(stats, list)
        assert isinstance(total_count, int)
        assert total_count > 0

        if len(stats) > 1:
            # Verify sorting by total messages in descending order
            assert stats[0]["messages"] >= stats[1]["messages"]

        # Test filtering by specific addresses
        # Using an address from our test data
        target_address = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"

        stats, count = fetch_stats_for_addresses(
            session=session,
            addresses=[target_address],
            sort_by=SortBy.MESSAGES,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=20,
        )

        # Should have stats for our target address
        assert len(stats) == 1
        assert stats[0]["address"] == target_address

        # Get the stats for our target address
        target_stats = stats[0]

        # Check counts match our test data
        assert (
            target_stats["messages"] >= 3
        )  # Should have at least 3 messages (1 post, 1 store, 1 program)
        assert target_stats["post"] >= 1
        assert target_stats["store"] >= 1
        assert target_stats["program"] >= 1

        # Test sorting by different message type
        stats, count = fetch_stats_for_addresses(
            session=session,
            addresses=None,
            sort_by=SortBy.store,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=20,
        )

        if len(stats) > 1:
            # Verify sorting by store count in descending order
            assert stats[0]["store"] >= stats[1]["store"]

        # Test with filters - first get post types to determine what filter to use
        stats, count = fetch_stats_for_addresses(
            session=session,
            addresses=None,
            sort_by=SortBy.MESSAGES,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=5,
        )

        if not stats:
            pytest.skip("No addresses in database to test filtering")

        # Find a message type that exists in the data
        filter_type = None
        filter_value = 1

        for stat in stats:
            for msg_type in [
                "post",
                "store",
                "aggregate",
                "program",
                "instance",
                "forget",
            ]:
                if stat[msg_type] > 0:
                    filter_type = getattr(SortBy, msg_type)
                    break
            if filter_type:
                break

        if not filter_type:
            pytest.skip("No message types with data to test filtering")

        # Now apply the filter
        filtered_stats, filtered_count = fetch_stats_for_addresses(
            session=session,
            addresses=None,
            sort_by=SortBy.MESSAGES,
            sort_order=SortOrder.DESCENDING,
            filters={filter_type: filter_value},
            page=1,
            per_page=20,
        )

        # If we got results, verify the filter worked
        if filtered_stats:
            if filter_type is not None:
                filter_column = str(filter_type.value).lower()
                for address_stat in filtered_stats:
                    assert address_stat[filter_column] >= filter_value

        # Test pagination
        per_page = 1
        stats_page1, count = fetch_stats_for_addresses(
            session=session,
            addresses=None,
            sort_by=SortBy.MESSAGES,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=per_page,
        )

        if count > per_page:
            stats_page2, _ = fetch_stats_for_addresses(
                session=session,
                addresses=None,
                sort_by=SortBy.MESSAGES,
                sort_order=SortOrder.DESCENDING,
                filters=None,
                page=2,
                per_page=per_page,
            )

            # Check that pagination works correctly
            assert len(stats_page1) <= per_page
            if len(stats_page1) > 0 and len(stats_page2) > 0:
                assert stats_page1[0]["address"] != stats_page2[0]["address"]
