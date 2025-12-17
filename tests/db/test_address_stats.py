"""
Tests for address stats functions.
"""

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.address import (
    find_matching_addresses,
    make_fetch_stats_address_query,
)
from aleph.db.accessors.messages import refresh_address_stats_mat_view
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
    """Test the make_fetch_stats_address_query function with various sorting and filtering options."""
    with session_factory() as session:
        test_messages = create_test_messages()
        session.add_all(test_messages)
        session.commit()

        # Refresh materialized views to ensure we have updated stats
        refresh_address_stats_mat_view(session)
        session.commit()

        # Test with default parameters (sort by total messages, descending)
        query = make_fetch_stats_address_query(
            addresses=None,
            sort_by=SortBy.messages,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=20,
        )

        # Execute the query
        result = session.execute(query)
        stats = [dict(row) for row in result.mappings().all()]

        # Get total count
        from sqlalchemy import func, select

        count_query = select(func.count()).select_from(
            make_fetch_stats_address_query(
                addresses=None,
                sort_by=SortBy.messages,
                sort_order=SortOrder.DESCENDING,
                filters=None,
                page=1,
                per_page=0,  # No pagination for count
            ).subquery()
        )
        total_count = session.execute(count_query).scalar_one()

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

        query = make_fetch_stats_address_query(
            addresses=[target_address],
            sort_by=SortBy.messages,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=20,
        )
        result = session.execute(query)
        filtered_stats = [dict(row) for row in result.mappings().all()]

        # Should have stats for our target address
        assert len(filtered_stats) == 1
        assert filtered_stats[0]["address"] == target_address

        # Get the stats for our target address
        target_stats = filtered_stats[0]

        # Check counts match our test data
        assert (
            target_stats["messages"] >= 3
        )  # Should have at least 3 messages (1 post, 1 store, 1 program)
        assert target_stats["post"] >= 1
        assert target_stats["store"] >= 1
        assert target_stats["program"] >= 1

        # Test sorting by different message type
        query = make_fetch_stats_address_query(
            addresses=None,
            sort_by=SortBy.store,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=20,
        )
        result = session.execute(query)
        sorted_stats = [dict(row) for row in result.mappings().all()]

        if len(sorted_stats) > 1:
            # Verify sorting by store count in descending order
            assert sorted_stats[0]["store"] >= sorted_stats[1]["store"]

        # Test with filters - first get all address stats to determine what filter to use
        query = make_fetch_stats_address_query(
            addresses=None,
            sort_by=SortBy.messages,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=5,
        )
        result = session.execute(query)
        stats = [dict(row) for row in result.mappings().all()]

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
                if stat.get(msg_type, 0) > 0:
                    filter_type = getattr(SortBy, msg_type)
                    break
            if filter_type:
                break

        if not filter_type:
            pytest.skip("No message types with data to test filtering")

        # Now apply the filter
        query = make_fetch_stats_address_query(
            addresses=None,
            sort_by=SortBy.messages,
            sort_order=SortOrder.DESCENDING,
            filters={filter_type: filter_value} if filter_type else None,
            page=1,
            per_page=20,
        )
        result = session.execute(query)
        filtered_stats_with_type = [dict(row) for row in result.mappings().all()]

        # If we got results, verify the filter worked
        if filtered_stats_with_type:
            if filter_type is not None:
                filter_column = str(filter_type.value).lower()
                for address_stat in filtered_stats_with_type:
                    assert address_stat[filter_column] >= filter_value

        # Test pagination
        per_page = 1
        query_page1 = make_fetch_stats_address_query(
            addresses=None,
            sort_by=SortBy.messages,
            sort_order=SortOrder.DESCENDING,
            filters=None,
            page=1,
            per_page=per_page,
        )
        result_page1 = session.execute(query_page1)
        stats_page1 = [dict(row) for row in result_page1.mappings().all()]

        count_query = select(func.count()).select_from(
            make_fetch_stats_address_query(
                addresses=None,
                sort_by=SortBy.messages,
                sort_order=SortOrder.DESCENDING,
                filters=None,
                page=1,
                per_page=0,  # No pagination for count
            ).subquery()
        )
        count = session.execute(count_query).scalar_one()

        if count > per_page:
            query_page2 = make_fetch_stats_address_query(
                addresses=None,
                sort_by=SortBy.messages,
                sort_order=SortOrder.DESCENDING,
                filters=None,
                page=2,
                per_page=per_page,
            )
            result_page2 = session.execute(query_page2)
            stats_page2 = [dict(row) for row in result_page2.mappings().all()]

            # Check that pagination works correctly
            assert len(stats_page1) <= per_page
            if len(stats_page1) > 0 and len(stats_page2) > 0:
                assert stats_page1[0]["address"] != stats_page2[0]["address"]
