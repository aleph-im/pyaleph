import datetime as dt
from typing import Optional

import pytest
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models.messages import MessageStatusDb, RemovedMessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

MESSAGES_URI = "/api/v0/messages.json"
MESSAGE_URI = "/api/v0/messages/{}"

OWNER_A = "0x8B8Ff2a2AC5d3b2Db4c1E7B1c1E7B1c1E7B1c1E7"
OWNER_B = "0x02c2A8B8Ff2a2AC5d3b2Db4c1E7B1c1E7B1c1E7B"
SENDER_A = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"
SENDER_B = "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"

STORE_CREDIT_HASH = "aaaa111111111111111111111111111111111111111111111111111111111111"
STORE_HOLD_HASH = "bbbb111111111111111111111111111111111111111111111111111111111111"
POST_HASH = "cccc111111111111111111111111111111111111111111111111111111111111"
LEGACY_STORE_HASH = "dddd111111111111111111111111111111111111111111111111111111111111"
REMOVING_STORE_HASH = "eeee111111111111111111111111111111111111111111111111111111111111"

REMOVED_AT_CREDIT = 1652786500.0
REMOVED_AT_HOLD = 1652787000.0
REMOVED_AT_POST = 1652787500.0


def _removed_snapshot(
    item_hash: str,
    message_type: MessageType,
    sender: str,
    owner: str,
    channel: str,
    time: float,
    payment_type: str,
    size: Optional[int],
    removed_at: Optional[float],
) -> tuple:
    snapshot = RemovedMessageDb(
        item_hash=item_hash,
        type=message_type,
        chain=Chain.ETH,
        sender=sender,
        signature=f"sig-{item_hash[:4]}",
        item_type=ItemType.inline,
        time=timestamp_to_datetime(time),
        channel=Channel(channel),
        owner=owner,
        payment_type=payment_type,
        size=size,
        removed_at=(
            timestamp_to_datetime(removed_at) if removed_at is not None else None
        ),
    )
    status = MessageStatusDb(
        item_hash=item_hash,
        status=MessageStatus.REMOVED,
        reception_time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
    )
    return snapshot, status


@pytest.fixture
def fixture_removed_messages(session_factory: DbSessionFactory):
    rows = [
        *_removed_snapshot(
            STORE_CREDIT_HASH,
            MessageType.store,
            sender=SENDER_A,
            owner=OWNER_A,
            channel="TEST",
            time=1652786100,
            payment_type="credit",
            size=4 * 1024 * 1024,
            removed_at=REMOVED_AT_CREDIT,
        ),
        *_removed_snapshot(
            STORE_HOLD_HASH,
            MessageType.store,
            sender=SENDER_B,
            owner=OWNER_B,
            channel="TEST",
            time=1652786200,
            payment_type="hold",
            size=10 * 1024 * 1024,
            removed_at=REMOVED_AT_HOLD,
        ),
        *_removed_snapshot(
            POST_HASH,
            MessageType.post,
            sender=SENDER_A,
            owner=OWNER_A,
            channel="OTHER",
            time=1652786300,
            payment_type="hold",
            size=None,
            removed_at=REMOVED_AT_POST,
        ),
        # Legacy row: moved by migration 0063 with an unknown removal time
        # and NULL payment_type (pre-coalescing removal)
        RemovedMessageDb(
            item_hash=LEGACY_STORE_HASH,
            type=MessageType.store,
            chain=Chain.ETH,
            sender=SENDER_B,
            signature="sig-dddd",
            item_type=ItemType.inline,
            time=timestamp_to_datetime(1652786400),
            channel=Channel("TEST"),
            owner=OWNER_B,
        ),
        MessageStatusDb(
            item_hash=LEGACY_STORE_HASH,
            status=MessageStatus.REMOVED,
            reception_time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
        ),
        # Phase-1 record of a message still REMOVING (size-only snapshot):
        # must never appear in the removed listing.
        RemovedMessageDb(
            item_hash=REMOVING_STORE_HASH,
            size=1024,
        ),
        MessageStatusDb(
            item_hash=REMOVING_STORE_HASH,
            status=MessageStatus.REMOVING,
            reception_time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
        ),
    ]

    with session_factory() as session:
        for row in rows:
            session.add(row)
        session.commit()

    return rows


@pytest.mark.asyncio
async def test_list_removed_messages(fixture_removed_messages, ccn_api_client):
    response = await ccn_api_client.get(MESSAGES_URI, params={"msgStatuses": "removed"})
    assert response.status == 200, await response.text()
    data = await response.json()

    messages = data["messages"]
    # The still-REMOVING phase-1 record is excluded
    assert data["pagination_total"] == 4
    # Sorted by removed_at DESC, legacy NULL rows last
    assert [msg["item_hash"] for msg in messages] == [
        POST_HASH,
        STORE_HOLD_HASH,
        STORE_CREDIT_HASH,
        LEGACY_STORE_HASH,
    ]

    credit_store = messages[2]
    assert credit_store["status"] == "removed"
    assert credit_store["type"] == "STORE"
    assert credit_store["chain"] == "ETH"
    assert credit_store["sender"] == SENDER_A
    assert credit_store["signature"] == "sig-aaaa"
    assert credit_store["item_type"] == "inline"
    assert credit_store["time"] == 1652786100.0
    assert credit_store["channel"] == "TEST"
    assert credit_store["owner"] == OWNER_A
    assert credit_store["payment_type"] == "credit"
    # size is the file-size snapshot taken while the message was alive
    assert credit_store["size"] == 4 * 1024 * 1024
    assert credit_store["removed_at"] == REMOVED_AT_CREDIT
    # Skeletons carry no content (the messages row is deleted at removal)
    assert "content" not in credit_store
    assert "item_content" not in credit_store

    legacy_store = messages[3]
    assert legacy_store["payment_type"] is None
    assert legacy_store["size"] is None
    assert legacy_store["removed_at"] is None


@pytest.mark.asyncio
async def test_list_removed_messages_ascending(
    fixture_removed_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "sortOrder": "1"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    # Ascending removed_at, legacy NULL rows still last
    assert [msg["item_hash"] for msg in data["messages"]] == [
        STORE_CREDIT_HASH,
        STORE_HOLD_HASH,
        POST_HASH,
        LEGACY_STORE_HASH,
    ]


@pytest.mark.asyncio
async def test_list_removed_messages_rejects_mixed_statuses(
    fixture_removed_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed,processed"}
    )
    assert response.status == 400, await response.text()

    # forgotten + removed cannot be combined either
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed,forgotten"}
    )
    assert response.status == 400, await response.text()


@pytest.mark.asyncio
async def test_list_removed_messages_rejects_cursor(
    fixture_removed_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "cursor": "some-cursor"}
    )
    assert response.status == 400, await response.text()


@pytest.mark.asyncio
async def test_list_removed_messages_rejects_unsupported_filters(
    fixture_removed_messages, ccn_api_client
):
    """Filters on data not preserved in removed_messages return 400."""
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
            MESSAGES_URI, params={"msgStatuses": "removed", **params}
        )
        assert response.status == 400, (params, await response.text())


@pytest.mark.asyncio
async def test_list_removed_messages_filters(fixture_removed_messages, ccn_api_client):
    # msgTypes
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "msgTypes": "STORE"}
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
        MESSAGES_URI, params={"msgStatuses": "removed", "owners": OWNER_B}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert {msg["item_hash"] for msg in data["messages"]} == {
        STORE_HOLD_HASH,
        LEGACY_STORE_HASH,
    }

    # addresses (sender)
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "addresses": SENDER_A}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert {msg["item_hash"] for msg in data["messages"]} == {
        STORE_CREDIT_HASH,
        POST_HASH,
    }

    # channels
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "channels": "OTHER"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [POST_HASH]

    # hashes
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={"msgStatuses": "removed", "hashes": STORE_CREDIT_HASH},
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [STORE_CREDIT_HASH]

    # paymentTypes: absence of a payment field means hold (billing
    # semantics), so the legacy NULL-payment row matches paymentTypes=hold
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "paymentTypes": "credit"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [STORE_CREDIT_HASH]

    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgStatuses": "removed", "paymentTypes": "hold"}
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
            "msgStatuses": "removed",
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
async def test_list_removed_messages_date_filters_use_removed_at(
    fixture_removed_messages, ccn_api_client
):
    # Window covers the credit and hold stores only; the legacy row without a
    # removal time is excluded, the POST row falls after the window.
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={
            "msgStatuses": "removed",
            "startDate": str(REMOVED_AT_CREDIT),
            "endDate": str(REMOVED_AT_POST),
        },
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert [msg["item_hash"] for msg in data["messages"]] == [
        STORE_HOLD_HASH,
        STORE_CREDIT_HASH,
    ]

    # The window matches the original message times (1652786100-400) but no
    # removed_at values: nothing is returned, proving removed_at is used.
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={
            "msgStatuses": "removed",
            "startDate": "1652786000",
            "endDate": "1652786450",
        },
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["messages"] == []


@pytest.mark.asyncio
async def test_list_removed_messages_pagination(
    fixture_removed_messages, ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI,
        params={"msgStatuses": "removed", "pagination": "2", "page": "2"},
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["pagination_total"] == 4
    assert data["pagination_page"] == 2
    assert [msg["item_hash"] for msg in data["messages"]] == [
        STORE_CREDIT_HASH,
        LEGACY_STORE_HASH,
    ]


@pytest.mark.asyncio
async def test_get_removed_message_status(fixture_removed_messages, ccn_api_client):
    """GET /messages/{hash} rebuilds REMOVED messages from their snapshot."""
    response = await ccn_api_client.get(MESSAGE_URI.format(STORE_CREDIT_HASH))
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["status"] == "removed"
    assert data["removed_at"] == REMOVED_AT_CREDIT
    assert data["size"] == 4 * 1024 * 1024
    assert data["message"]["item_hash"] == STORE_CREDIT_HASH
    assert data["message"]["owner"] == OWNER_A
    assert data["message"]["payment_type"] == "credit"
    assert data["message"]["time"] == 1652786100.0
    # The messages row is gone: skeletons carry no content
    assert "content" not in data["message"]

    # Legacy removal: record fields are null
    response = await ccn_api_client.get(MESSAGE_URI.format(LEGACY_STORE_HASH))
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["status"] == "removed"
    assert data["removed_at"] is None
    assert data["size"] is None
