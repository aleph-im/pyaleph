import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models.messages import ForgottenMessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory

MESSAGES_URI = "/api/v0/messages.json"

OWNER_A = "0x8B8Ff2a2AC5d3b2Db4c1E7B1c1E7B1c1E7B1c1E7"
OWNER_B = "0x02c2A8B8Ff2a2AC5d3b2Db4c1E7B1c1E7B1c1E7B"
SENDER_A = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
SENDER_B = "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"

STORE_CREDIT_HASH = "1111111111111111111111111111111111111111111111111111111111111111"
STORE_HOLD_HASH = "2222222222222222222222222222222222222222222222222222222222222222"
POST_HASH = "3333333333333333333333333333333333333333333333333333333333333333"
LEGACY_STORE_HASH = "4444444444444444444444444444444444444444444444444444444444444444"

FORGOTTEN_AT_CREDIT = 1652786500.0
FORGOTTEN_AT_HOLD = 1652787000.0
FORGOTTEN_AT_POST = 1652787500.0


@pytest.fixture
def fixture_forgotten_messages(session_factory: DbSessionFactory):
    rows = [
        ForgottenMessageDb(
            item_hash=STORE_CREDIT_HASH,
            type=MessageType.store,
            chain=Chain.ETH,
            sender=SENDER_A,
            signature="sig-1",
            item_type=ItemType.inline,
            time=timestamp_to_datetime(1652786100),
            channel=Channel("TEST"),
            forgotten_by=["aaaa" * 16],
            owner=OWNER_A,
            payment_type="credit",
            size=4 * 1024 * 1024,
            forgotten_at=timestamp_to_datetime(FORGOTTEN_AT_CREDIT),
        ),
        ForgottenMessageDb(
            item_hash=STORE_HOLD_HASH,
            type=MessageType.store,
            chain=Chain.ETH,
            sender=SENDER_B,
            signature="sig-2",
            item_type=ItemType.inline,
            time=timestamp_to_datetime(1652786200),
            channel=Channel("TEST"),
            forgotten_by=["bbbb" * 16],
            owner=OWNER_B,
            payment_type="hold",
            size=10 * 1024 * 1024,
            forgotten_at=timestamp_to_datetime(FORGOTTEN_AT_HOLD),
        ),
        ForgottenMessageDb(
            item_hash=POST_HASH,
            type=MessageType.post,
            chain=Chain.ETH,
            sender=SENDER_A,
            signature="sig-3",
            item_type=ItemType.inline,
            time=timestamp_to_datetime(1652786300),
            channel=Channel("OTHER"),
            forgotten_by=["cccc" * 16],
            owner=OWNER_A,
            payment_type=None,
            size=None,
            forgotten_at=timestamp_to_datetime(FORGOTTEN_AT_POST),
        ),
        # Legacy row: forgotten before the metadata columns existed
        ForgottenMessageDb(
            item_hash=LEGACY_STORE_HASH,
            type=MessageType.store,
            chain=Chain.ETH,
            sender=SENDER_B,
            signature="sig-4",
            item_type=ItemType.inline,
            time=timestamp_to_datetime(1652786400),
            channel=Channel("TEST"),
            forgotten_by=["dddd" * 16],
        ),
    ]

    with session_factory() as session:
        for row in rows:
            session.add(row)
        session.commit()

    return rows


@pytest.mark.asyncio
async def test_list_forgotten_messages(fixture_forgotten_messages, ccn_api_client):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    messages = data["messages"]
    assert data["pagination_total"] == 4
    # Sorted by forgotten_at DESC, legacy NULL rows last
    assert [msg["item_hash"] for msg in messages] == [
        POST_HASH,
        STORE_HOLD_HASH,
        STORE_CREDIT_HASH,
        LEGACY_STORE_HASH,
    ]

    credit_store = messages[2]
    assert credit_store["status"] == "forgotten"
    assert credit_store["type"] == "STORE"
    assert credit_store["chain"] == "ETH"
    assert credit_store["sender"] == SENDER_A
    assert credit_store["signature"] == "sig-1"
    assert credit_store["item_type"] == "inline"
    assert credit_store["time"] == 1652786100.0
    assert credit_store["channel"] == "TEST"
    assert credit_store["forgotten_by"] == ["aaaa" * 16]
    assert credit_store["owner"] == OWNER_A
    assert credit_store["payment_type"] == "credit"
    assert credit_store["size"] == 4 * 1024 * 1024
    assert credit_store["forgotten_at"] == FORGOTTEN_AT_CREDIT

    legacy_store = messages[3]
    assert legacy_store["owner"] is None
    assert legacy_store["payment_type"] is None
    assert legacy_store["size"] is None
    assert legacy_store["forgotten_at"] is None


@pytest.mark.asyncio
async def test_list_forgotten_messages_ascending(
    fixture_forgotten_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "sortOrder": "1"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    # Ascending forgotten_at, legacy NULL rows still last
    assert [msg["item_hash"] for msg in data["messages"]] == [
        STORE_CREDIT_HASH,
        STORE_HOLD_HASH,
        POST_HASH,
        LEGACY_STORE_HASH,
    ]


@pytest.mark.asyncio
async def test_list_forgotten_messages_rejects_mixed_statuses(
    fixture_forgotten_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten,processed"}
    )
    assert response.status == 400, await response.text()


@pytest.mark.asyncio
async def test_list_forgotten_messages_rejects_cursor(
    fixture_forgotten_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "cursor": "some-cursor"}
    )
    assert response.status == 400, await response.text()


@pytest.mark.asyncio
async def test_list_forgotten_messages_filters(
    fixture_forgotten_messages, ccn_api_client
):
    # msgTypes
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "msgTypes": "STORE"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert {msg["item_hash"] for msg in data["messages"]} == {
        STORE_CREDIT_HASH,
        STORE_HOLD_HASH,
        LEGACY_STORE_HASH,
    }

    # owners
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "owners": OWNER_B}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [STORE_HOLD_HASH]

    # addresses (sender)
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "addresses": SENDER_A}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert {msg["item_hash"] for msg in data["messages"]} == {
        STORE_CREDIT_HASH,
        POST_HASH,
    }

    # channels
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "channels": "OTHER"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [POST_HASH]

    # hashes
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={"msgStatuses": "forgotten", "hashes": STORE_CREDIT_HASH},
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [STORE_CREDIT_HASH]

    # paymentTypes: absence of a payment field means hold (billing
    # semantics), so NULL-payment rows (legacy included) match
    # paymentTypes=hold — same coalescing as the removed query
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "paymentTypes": "credit"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [STORE_CREDIT_HASH]

    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "forgotten", "paymentTypes": "hold"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert {msg["item_hash"] for msg in data["messages"]} == {
        STORE_HOLD_HASH,
        POST_HASH,
        LEGACY_STORE_HASH,
    }

    # combined with msgTypes=STORE: the implicit-hold legacy STORE matches
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={
            "msgStatuses": "forgotten",
            "paymentTypes": "hold",
            "msgTypes": "STORE",
        },
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert {msg["item_hash"] for msg in data["messages"]} == {
        STORE_HOLD_HASH,
        LEGACY_STORE_HASH,
    }


@pytest.mark.asyncio
async def test_list_forgotten_messages_rejects_unsupported_filters(
    fixture_forgotten_messages, ccn_api_client
):
    """Filters on data not preserved in forgotten_messages return 400."""
    unsupported_params = [
        {"refs": "some-ref"},
        {"contentTypes": "file"},
        {"contentHashes": STORE_CREDIT_HASH},
        {"contentKeys": STORE_CREDIT_HASH},
        {"tags": "mainnet"},
        {"startBlock": "100"},
        {"endBlock": "200"},
    ]
    for params in unsupported_params:
        response = await ccn_api_client.get(
            MESSAGES_URI, params={"msgStatuses": "forgotten", **params}
        )
        assert response.status == 400, (params, await response.text())


@pytest.mark.asyncio
async def test_list_forgotten_messages_date_filters_use_forgotten_at(
    fixture_forgotten_messages, ccn_api_client
):
    # Window covers the credit and hold stores only; the legacy NULL row is
    # excluded by the date filter, the POST row falls after the window.
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={
            "msgStatuses": "forgotten",
            "startDate": str(FORGOTTEN_AT_CREDIT),
            "endDate": str(FORGOTTEN_AT_POST),
        },
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [
        STORE_HOLD_HASH,
        STORE_CREDIT_HASH,
    ]

    # The window does not match the original message times (1652786100-400):
    # filtering on that range returns nothing, proving forgotten_at is used.
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={
            "msgStatuses": "forgotten",
            "startDate": "1652786000",
            "endDate": "1652786450",
        },
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["messages"] == []


@pytest.mark.asyncio
async def test_list_forgotten_messages_pagination(
    fixture_forgotten_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={"msgStatuses": "forgotten", "pagination": "2", "page": "2"},
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["pagination_total"] == 4
    assert data["pagination_page"] == 2
    assert [msg["item_hash"] for msg in data["messages"]] == [
        STORE_CREDIT_HASH,
        LEGACY_STORE_HASH,
    ]
