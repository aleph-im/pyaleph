import datetime as dt
from typing import List

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.accessors.messages import refresh_address_stats_mat_view
from aleph.db.models import MessageDb
from aleph.db.models.messages import MessageStatusDb
from aleph.toolkit.timestamp import utc_now
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

ADDRESSES_STATS_URI_V1 = "/api/v1/addresses/stats.json"


@pytest.fixture
def test_addresses():
    """Return a list of test addresses used in the message fixtures."""
    return [
        "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",  # Has POST, STORE, PROGRAM
        "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",  # Has POST
        "0x5D00fAD0763A876202a29FE71D30B4554D28FB97",  # Has STORE
        "0xDifferentAddress1",  # Has AGGREGATE
        "0xDifferentAddress2",  # Has INSTANCE
    ]


@pytest.fixture
def fixture_address_stats_messages(
    session_factory: DbSessionFactory, test_addresses
) -> List[MessageDb]:
    """Create test messages with different types and addresses for address stats testing."""
    now = utc_now()
    messages = [
        # First address has multiple message types
        MessageDb(
            item_hash="hash1",
            chain=Chain.ETH,
            sender=test_addresses[0],
            signature="0xsig1",
            item_type=ItemType.inline,
            type=MessageType.post,
            content={"test": "content1"},
            size=100,
            time=now,
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="hash2",
            chain=Chain.ETH,
            sender=test_addresses[0],
            signature="0xsig2",
            item_type=ItemType.inline,
            type=MessageType.store,
            content={"test": "content2"},
            size=100,
            time=now + dt.timedelta(seconds=1),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="hash3",
            chain=Chain.ETH,
            sender=test_addresses[0],
            signature="0xsig3",
            item_type=ItemType.inline,
            type=MessageType.program,
            content={"test": "content3"},
            size=100,
            time=now + dt.timedelta(seconds=2),
            channel=Channel("TEST"),
        ),
        # Second address has only POST
        MessageDb(
            item_hash="hash4",
            chain=Chain.ETH,
            sender=test_addresses[1],
            signature="0xsig4",
            item_type=ItemType.inline,
            type=MessageType.post,
            content={"test": "content4"},
            size=100,
            time=now + dt.timedelta(seconds=3),
            channel=Channel("TEST"),
        ),
        # Third address has only STORE
        MessageDb(
            item_hash="hash5",
            chain=Chain.ETH,
            sender=test_addresses[2],
            signature="0xsig5",
            item_type=ItemType.inline,
            type=MessageType.store,
            content={"test": "content5"},
            size=100,
            time=now + dt.timedelta(seconds=4),
            channel=Channel("TEST"),
        ),
        # Fourth address has AGGREGATE
        MessageDb(
            item_hash="hash6",
            chain=Chain.ETH,
            sender=test_addresses[3],
            signature="0xsig6",
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            content={"key": "test6", "content": {"data": "aggregate"}},
            size=100,
            time=now + dt.timedelta(seconds=5),
            channel=Channel("TEST"),
        ),
        # Fifth address has INSTANCE
        MessageDb(
            item_hash="hash7",
            chain=Chain.ETH,
            sender=test_addresses[4],
            signature="0xsig7",
            item_type=ItemType.inline,
            type=MessageType.instance,
            content={"test": "content7"},
            size=100,
            time=now + dt.timedelta(seconds=6),
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

        # Refresh the materialized view to ensure stats are up to date
        refresh_address_stats_mat_view(session)
        session.commit()

    return messages


@pytest.mark.asyncio
async def test_address_stats_endpoint_basic(
    ccn_api_client, fixture_address_stats_messages, test_addresses
):
    """Test the basic functionality of the addresses stats endpoint."""
    response = await ccn_api_client.get(ADDRESSES_STATS_URI_V1)

    assert response.status == 200, await response.text()
    data = await response.json()

    # Verify the response structure
    assert "data" in data
    assert "pagination_page" in data
    assert "pagination_per_page" in data
    assert "pagination_total" in data
    assert "pagination_item" in data

    # Check pagination information
    assert data["pagination_item"] == "addresses"
    assert data["pagination_page"] == 1
    assert data["pagination_per_page"] > 0
    assert data["pagination_total"] >= 5  # At least our 5 test addresses

    # Check stats data
    assert isinstance(data["data"], dict)
    assert len(data["data"]) > 0

    # Verify structure of a stats item
    first_address = next(iter(data["data"]))
    first_item = data["data"][first_address]
    assert "total" in first_item
    assert "post" in first_item
    assert "store" in first_item
    assert "program" in first_item
    assert "aggregate" in first_item
    assert "instance" in first_item
    assert "forget" in first_item

    # Verify actual values for all test addresses
    expected_stats = {
        test_addresses[0]: {
            "total": 3,
            "post": 1,
            "store": 1,
            "program": 1,
            "aggregate": 0,
            "instance": 0,
            "forget": 0,
        },
        test_addresses[1]: {
            "total": 1,
            "post": 1,
            "store": 0,
            "program": 0,
            "aggregate": 0,
            "instance": 0,
            "forget": 0,
        },
        test_addresses[2]: {
            "total": 1,
            "post": 0,
            "store": 1,
            "program": 0,
            "aggregate": 0,
            "instance": 0,
            "forget": 0,
        },
        test_addresses[3]: {
            "total": 1,
            "post": 0,
            "store": 0,
            "program": 0,
            "aggregate": 1,
            "instance": 0,
            "forget": 0,
        },
        test_addresses[4]: {
            "total": 1,
            "post": 0,
            "store": 0,
            "program": 0,
            "aggregate": 0,
            "instance": 1,
            "forget": 0,
        },
    }

    for address, expected in expected_stats.items():
        assert address in data["data"]
        assert data["data"][address] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "per_page, page1, page2",
    [
        (2, 1, 2),
        (1, 1, 2),
        (0, 1, 2),  # all items in one page
    ],
)
async def test_address_stats_pagination(
    ccn_api_client,
    fixture_address_stats_messages,
    per_page,
    page1,
    page2,
):
    """Test pagination of the addresses stats endpoint."""

    response_page1 = await ccn_api_client.get(
        f"{ADDRESSES_STATS_URI_V1}?pagination={per_page}&page={page1}"
    )
    assert response_page1.status == 200, await response_page1.text()
    data_page1 = await response_page1.json()

    response_page2 = await ccn_api_client.get(
        f"{ADDRESSES_STATS_URI_V1}?pagination={per_page}&page={page2}"
    )
    assert response_page2.status == 200, await response_page2.text()
    data_page2 = await response_page2.json()

    # Basic pagination assertions
    assert data_page1["pagination_page"] == page1

    if per_page > 0:
        assert data_page1["pagination_per_page"] == per_page
        assert data_page2["pagination_page"] == page2

        # Should not return the same addresses across pages
        page1_addresses = set(data_page1["data"].keys())
        page2_addresses = set(data_page2["data"].keys())

        assert len(page1_addresses.intersection(page2_addresses)) == 0

    else:
        # per_page == 0 then everything in one page
        assert data_page1["pagination_per_page"] == 0
        assert len(data_page2["data"]) == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sort_by, sort_order, field, comparator",
    [
        ("total", -1, "total", lambda a, b: a >= b),  # DSC Sort
        ("post", -1, "post", lambda a, b: a >= b),  # DSC Sort
        ("total", 1, "total", lambda a, b: a <= b),  # ASC Sort
    ],
)
async def test_address_stats_sorting(
    ccn_api_client,
    fixture_address_stats_messages,
    test_addresses,
    sort_by,
    sort_order,
    field,
    comparator,
):
    """Test sorting functionality of the addresses stats endpoint."""
    response = await ccn_api_client.get(
        f"{ADDRESSES_STATS_URI_V1}?sort_by={sort_by}&sortOrder={sort_order}"
    )
    assert response.status == 200

    payload = await response.json()
    data = list(payload["data"].values())

    if len(data) >= 2:
        assert comparator(data[0][field], data[1][field])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address_contains, expected_count",
    [
        ("0x", 5),  # matches all addresses
        ("Different", 2),  # matches only DifferentAddress1 & 2
        ("NO_MATCH", 0),  # matches none
    ],
)
async def test_address_stats_filtering(
    ccn_api_client,
    fixture_address_stats_messages,
    address_contains,
    expected_count,
):
    """Test filtering functionality of the addresses stats endpoint."""

    response = await ccn_api_client.get(
        f"{ADDRESSES_STATS_URI_V1}?addressContains={address_contains}"
    )
    assert response.status == 200

    payload = await response.json()
    data = payload["data"]

    # All returned addresses must contain the substring (case-insensitive)
    for address in data:
        assert address_contains.lower() in address.lower()

    # Exact expectations based on fixture
    assert payload["pagination_total"] == expected_count
    assert len(data) == expected_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address_index, expected_fields",
    [
        # Address with multiple message types
        (
            0,
            {
                "post": 1,
                "store": 1,
                "program": 1,
                "total": 3,
            },
        ),
        # Aggregate-only address
        (
            3,
            {
                "aggregate": 1,
            },
        ),
        # Instance-only address
        (
            4,
            {
                "instance": 1,
            },
        ),
    ],
)
async def test_address_stats_all_message_types(
    ccn_api_client,
    fixture_address_stats_messages,
    test_addresses,
    address_index,
    expected_fields,
):
    """Test that all message types are correctly represented in the address stats API."""

    address = test_addresses[address_index]

    response = await ccn_api_client.get(
        f"{ADDRESSES_STATS_URI_V1}?addressContains={address}"
    )
    assert response.status == 200

    payload = await response.json()
    data = payload["data"]

    # Should have exactly one result
    assert len(data) == 1
    assert address in data
    stats = data[address]

    # Verify expected message type counts
    for field, min_value in expected_fields.items():
        assert (
            stats[field] >= min_value
        ), f"Expected {field} >= {min_value}, got {stats[field]}"


@pytest.mark.asyncio
async def test_address_stats_request_all_items(
    ccn_api_client, fixture_address_stats_messages
):
    """Test requesting all items without pagination."""
    # Get count of all addresses
    response_normal = await ccn_api_client.get(ADDRESSES_STATS_URI_V1)
    assert response_normal.status == 200
    data_normal = await response_normal.json()
    total_count = data_normal["pagination_total"]

    # Request all items with pagination=0
    response_all = await ccn_api_client.get(ADDRESSES_STATS_URI_V1 + "?pagination=0")
    assert response_all.status == 200
    data_all = await response_all.json()

    # Should return all items
    assert len(data_all["data"]) == total_count
    assert data_all["pagination_per_page"] == 0
    assert data_all["pagination_total"] == total_count


@pytest.mark.asyncio
async def test_address_stats_invalid_params(
    ccn_api_client, fixture_address_stats_messages
):
    """Test the endpoint with invalid parameters."""
    # Invalid pagination (negative)
    response_invalid_pagination = await ccn_api_client.get(
        ADDRESSES_STATS_URI_V1 + "?pagination=-1"
    )
    assert response_invalid_pagination.status == 422  # Unprocessable entity

    # Invalid page (negative)
    response_invalid_page = await ccn_api_client.get(
        ADDRESSES_STATS_URI_V1 + "?page=-1"
    )
    assert response_invalid_page.status == 422

    # Invalid sort_by
    response_invalid_sort = await ccn_api_client.get(
        ADDRESSES_STATS_URI_V1 + "?sortBy=invalid_field"
    )
    assert response_invalid_sort.status == 422

    # Invalid sort_order
    response_invalid_order = await ccn_api_client.get(
        ADDRESSES_STATS_URI_V1 + "?sortOrder=INVALID"
    )
    assert response_invalid_order.status == 422
