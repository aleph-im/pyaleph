import datetime as dt
import json

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

from aleph.db.models import AlephCreditBalanceDb, MessageStatusDb, PendingMessageDb
from aleph.schemas.message_content import ContentSource, MessageContent
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import ErrorCode, MessageStatus
from aleph.web.controllers.app_state_getters import APP_STATE_STORAGE_SERVICE

# Note: fixture_vprogram_message and user_credit_balance are redefined here
# (rather than imported from tests/message_processing/test_process_vprograms.py)
# because pytest fixture functions imported across test modules are flagged as
# unused imports by ruff and get stripped by `hatch run linting:fmt`. Keep the
# canonical values identical to the originals.

SENDER = VPROGRAM_CONTENT["address"]

PRICE_URI = f"/api/v0/price/{VPROGRAM_ITEM_HASH}"
PRICE_ESTIMATE_URI = "/api/v0/price/estimate"
MESSAGES_URI = "/api/v0/messages.json"
MESSAGE_URI = f"/api/v0/messages/{VPROGRAM_ITEM_HASH}"


@pytest.fixture
def fixture_vprogram_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash=VPROGRAM_ITEM_HASH,
        type=MessageType.v_program,
        chain=Chain.ETH,
        sender=SENDER,
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(VPROGRAM_CONTENT),
        time=timestamp_to_datetime(1719502000.0),
        channel=None,
        reception_time=timestamp_to_datetime(1719502001),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2026, 1, 1),
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()
    return pending_message


@pytest.fixture
def user_credit_balance(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        session.add(
            AlephCreditBalanceDb(
                address=SENDER,
                credit_ref="test-credit-ref",
                credit_index=0,
                amount_remaining=1_000_000_000,
                expiration_date=None,
                message_timestamp=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.commit()


@pytest.mark.asyncio
async def test_vprogram_price_estimate(
    ccn_api_client,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # The shared ccn_api_client fixture wires up a fully mocked storage
    # service (see tests/conftest.py's ccn_test_aiohttp_app), so inline
    # content resolution needs to be stubbed here to return the actual
    # V-PROGRAM content instead of an unconfigured AsyncMock.
    raw_content = json.dumps(VPROGRAM_CONTENT, separators=(",", ":"))
    storage_service = ccn_api_client.app[APP_STATE_STORAGE_SERVICE]
    storage_service.get_message_content.return_value = MessageContent(
        hash=VPROGRAM_ITEM_HASH,
        source=ContentSource.INLINE,
        value=VPROGRAM_CONTENT,
        raw_value=raw_content,
    )

    message = {
        "chain": "ETH",
        "sender": VPROGRAM_CONTENT["address"],
        "type": "V-PROGRAM",
        "channel": "TEST",
        "time": 1719502000.0,
        "item_type": "inline",
        "item_hash": VPROGRAM_ITEM_HASH,
        "item_content": raw_content,
    }
    response = await ccn_api_client.post(PRICE_ESTIMATE_URI, json={"message": message})
    assert response.status == 200, await response.text()
    result = await response.json()
    assert float(result["cost"]) > 0
    assert result["payment_type"] == "credit"


@pytest.mark.asyncio
async def test_vprogram_message_price(
    ccn_api_client,
    session_factory,
    message_processor,
    fixture_vprogram_message,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(PRICE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert float(result["cost"]) > 0
    assert result["payment_type"] == "credit"


@pytest.mark.asyncio
async def test_vprogram_in_messages_list(
    ccn_api_client,
    session_factory,
    message_processor,
    fixture_vprogram_message,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    # Filter by msgType (singular, deprecated but still supported).
    response = await ccn_api_client.get(MESSAGES_URI, params={"msgType": "V-PROGRAM"})
    assert response.status == 200, await response.text()
    messages = (await response.json())["messages"]
    assert len(messages) == 1
    assert messages[0]["item_hash"] == VPROGRAM_ITEM_HASH
    assert messages[0]["content"]["verification"]["backend"] == "sev_snp"

    # Filter by msgTypes (plural).
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgTypes": "V-PROGRAM,INSTANCE"}
    )
    assert response.status == 200, await response.text()
    assert any(
        m["item_hash"] == VPROGRAM_ITEM_HASH
        for m in (await response.json())["messages"]
    )

    # Single-message endpoint.
    response = await ccn_api_client.get(MESSAGE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert result["status"] == "processed"
    assert result["message"]["item_hash"] == VPROGRAM_ITEM_HASH
    assert result["message"]["content"]["verification"]["backend"] == "sev_snp"

    # Headers content format must not crash on the new type.
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"msgType": "V-PROGRAM", "contentFormat": "headers"}
    )
    assert response.status == 200, await response.text()
    headers_messages = (await response.json())["messages"]
    assert len(headers_messages) == 1
    assert headers_messages[0]["content"] == {"address": SENDER}


@pytest.mark.asyncio
async def test_vprogram_pending_display(
    ccn_api_client,
    fixture_vprogram_message,
):
    # Not processed yet: the message must be visible as pending.
    response = await ccn_api_client.get(MESSAGE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert result["status"] == "pending"
    assert result["messages"][0]["item_hash"] == VPROGRAM_ITEM_HASH


@pytest.mark.asyncio
async def test_vprogram_rejected_display(
    ccn_api_client,
    session_factory,
    message_processor,
    fixture_vprogram_message,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # No credit balance: processing rejects the message, and the rejected
    # message is still visible through the single-message endpoint.
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    response = await ccn_api_client.get(MESSAGE_URI)
    assert response.status == 200, await response.text()
    result = await response.json()
    assert result["status"] == "rejected"
    assert result["error_code"] == ErrorCode.CREDIT_INSUFFICIENT.value
