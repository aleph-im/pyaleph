"""
Tests for address stats functions.
"""

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.address import count_address_stats, make_address_filter_subquery
from aleph.db.accessors.messages import (
    get_message_stats_by_address,
    refresh_address_stats_mat_view,
)
from aleph.db.models import MessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortByMessageType, SortOrder


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
async def test_make_address_filter_subquery():
    """Test the subquery builder for address filtering."""
    # Test with a simple pattern
    subquery = make_address_filter_subquery("0x123")

    assert subquery.element.froms[0].name == "address_stats_mat_view"
    assert "lower(address_stats_mat_view.address)" in str(subquery.element)

    # Test with a more complex pattern
    subquery = make_address_filter_subquery("SPECIAL_CHARS!@#")

    # Structure should be the same
    assert "lower(address_stats_mat_view.address)" in str(subquery.element)

    # Test with empty pattern
    subquery = make_address_filter_subquery("")

    # Should still create a valid subquery with empty pattern
    assert "lower(address_stats_mat_view.address)" in str(subquery.element)


@pytest.mark.asyncio
async def test_count_address_stats(session_factory: DbSessionFactory):
    """Test the count_address_stats function."""
    with session_factory() as session:
        test_messages = create_test_messages()
        session.add_all(test_messages)
        session.commit()

        # Refresh views to ensure we have updated stats
        refresh_address_stats_mat_view(session)
        session.commit()

        # Count all addresses
        total_count = count_address_stats(session)
        assert (
            total_count >= 3
        )  # We have at least 3 distinct addresses in our test data

        # Count with filter
        filtered_count = count_address_stats(session, address_contains="0x69")
        assert filtered_count >= 1  # Should find at least one address

        # Count with filter that should match no addresses
        no_match_count = count_address_stats(session, address_contains="NOMATCH")
        assert no_match_count == 0


@pytest.mark.asyncio
async def test_fetch_stats_address_query(session_factory: DbSessionFactory):
    """Test the get_message_stats_by_address function with various sorting and filtering options."""
    with session_factory() as session:
        test_messages = create_test_messages()
        session.add_all(test_messages)
        session.commit()

        # Refresh materialized views to ensure we have updated stats
        refresh_address_stats_mat_view(session)
        session.commit()

        # Test with default parameters (sort by total messages, descending)
        stats = get_message_stats_by_address(
            session=session,
            sort_by=SortByMessageType.TOTAL,
            sort_order=SortOrder.DESCENDING,
            page=1,
            pagination=20,
        )

        # Get total count
        total_count = count_address_stats(session)

        # Check stats structure
        assert isinstance(stats, list)
        assert isinstance(total_count, int)
        assert total_count > 0

        if len(stats) > 1:
            # Verify sorting by total messages in descending order
            assert stats[0].total >= stats[1].total

        # Test filtering by address_contains
        filtered_stats = get_message_stats_by_address(
            session=session,
            address_contains="0x69",
            sort_by=SortByMessageType.TOTAL,
            sort_order=SortOrder.DESCENDING,
            page=1,
            pagination=20,
        )

        # Should have at least one result
        assert len(filtered_stats) >= 1

        # All results should contain the filter string
        for stat in filtered_stats:
            assert "0x69" in stat.address.lower()

        # Test with different sort_by
        program_stats = get_message_stats_by_address(
            session=session,
            sort_by=SortByMessageType.PROGRAM,
            sort_order=SortOrder.DESCENDING,
            page=1,
            pagination=20,
        )

        if len(program_stats) > 1:
            # Verify sorting by program count
            assert program_stats[0].program >= program_stats[1].program

        # Test with ascending sort order
        asc_stats = get_message_stats_by_address(
            session=session,
            sort_by=SortByMessageType.TOTAL,
            sort_order=SortOrder.ASCENDING,
            page=1,
            pagination=20,
        )

        for i, stat in enumerate(asc_stats):
            print(f"  {i}: address={stat.address}, total={stat.total}")

        if len(asc_stats) > 1:
            # Verify sorting in ascending order
            assert asc_stats[0].total <= asc_stats[1].total

        # Test pagination
        pagination = 1
        stats_page1 = get_message_stats_by_address(
            session=session,
            sort_by=SortByMessageType.TOTAL,
            sort_order=SortOrder.DESCENDING,
            page=1,
            pagination=pagination,
        )

        # Should have exactly pagination items
        assert len(stats_page1) <= pagination

        # If there are more addresses, test next page
        if total_count > pagination:
            stats_page2 = get_message_stats_by_address(
                session=session,
                sort_by=SortByMessageType.TOTAL,
                sort_order=SortOrder.DESCENDING,
                page=2,
                pagination=pagination,
            )

            # Check that pagination works correctly
            assert len(stats_page2) <= pagination
            if len(stats_page1) > 0 and len(stats_page2) > 0:
                assert stats_page1[0].address != stats_page2[0].address


@pytest.mark.asyncio
async def test_zero_per_page_returns_all(session_factory: DbSessionFactory):
    """Test that setting pagination=0 returns all results without pagination."""
    with session_factory() as session:
        test_messages = create_test_messages()
        session.add_all(test_messages)
        session.commit()

        # Refresh materialized views to ensure we have updated stats
        refresh_address_stats_mat_view(session)
        session.commit()

        # Count total addresses
        total_count = count_address_stats(session)

        # Get all results with pagination=0
        all_stats = get_message_stats_by_address(
            session=session,
            sort_by=SortByMessageType.TOTAL,
            sort_order=SortOrder.DESCENDING,
            page=1,
            pagination=0,  # This should return all results
        )

        # Should have all addresses
        assert len(all_stats) == total_count
