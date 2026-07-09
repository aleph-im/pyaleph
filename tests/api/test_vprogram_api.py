import datetime as dt
import json

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

from aleph.db.models import AlephCreditBalanceDb, MessageStatusDb, PendingMessageDb
from aleph.schemas.message_content import ContentSource, MessageContent
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus
from aleph.web.controllers.app_state_getters import APP_STATE_STORAGE_SERVICE

# Note: fixture_vprogram_message and user_credit_balance are redefined here
# (rather than imported from tests/message_processing/test_process_vprograms.py)
# because pytest fixture functions imported across test modules are flagged as
# unused imports by ruff and get stripped by `hatch run linting:fmt`. Keep the
# canonical values identical to the originals.

SENDER = VPROGRAM_CONTENT["address"]

PRICE_URI = f"/api/v0/price/{VPROGRAM_ITEM_HASH}"
PRICE_ESTIMATE_URI = "/api/v0/price/estimate"


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
