import datetime as dt

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from aleph_message.models.execution.base import Payment, PaymentType

from aleph.db.models.messages import ForgottenMessageDb, MessageStatusDb
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.services.cost import get_total_and_detailed_costs
from aleph.toolkit.constants import MIN_STORE_COST_MIB, MiB
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

PRICE_URI = "/api/v0/price/{}"

OWNER = "0xB6B5358493AF8159B17506C5cC85df69193444BC"
SENDER = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106"

FORGOTTEN_CREDIT_STORE_HASH = (
    "5555555555555555555555555555555555555555555555555555555555555555"
)
FORGOTTEN_LEGACY_STORE_HASH = (
    "6666666666666666666666666666666666666666666666666666666666666666"
)
FORGOTTEN_POST_HASH = "7777777777777777777777777777777777777777777777777777777777777777"
FORGOTTEN_HOLD_STORE_HASH = (
    "8888888888888888888888888888888888888888888888888888888888888888"
)
FORGOTTEN_NULL_PAYMENT_STORE_HASH = (
    "9999999999999999999999999999999999999999999999999999999999999999"
)

SMALL_FILE_SIZE = 1024 * 1024  # 1 MiB, well below the MIN_STORE_COST_MIB floor


def _add_forgotten_message(session, **kwargs) -> None:
    defaults = dict(
        chain=Chain.ETH,
        sender=SENDER,
        signature="sig",
        item_type=ItemType.inline,
        time=timestamp_to_datetime(1652786100),
        channel=Channel("TEST"),
        forgotten_by=["aaaa" * 16],
    )
    row = ForgottenMessageDb(**{**defaults, **kwargs})
    session.add(row)
    session.add(
        MessageStatusDb(
            item_hash=row.item_hash,
            status=MessageStatus.FORGOTTEN,
            reception_time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
        )
    )


@pytest.fixture
def fixture_forgotten_messages_for_pricing(session_factory: DbSessionFactory):
    with session_factory() as session:
        _add_forgotten_message(
            session,
            item_hash=FORGOTTEN_CREDIT_STORE_HASH,
            type=MessageType.store,
            owner=OWNER,
            payment_type="credit",
            size=SMALL_FILE_SIZE,
            forgotten_at=timestamp_to_datetime(1652786500),
        )
        # Legacy row: no billing metadata
        _add_forgotten_message(
            session,
            item_hash=FORGOTTEN_LEGACY_STORE_HASH,
            type=MessageType.store,
        )
        # Forgotten non-STORE message with metadata-like fields
        _add_forgotten_message(
            session,
            item_hash=FORGOTTEN_POST_HASH,
            type=MessageType.post,
            owner=OWNER,
            forgotten_at=timestamp_to_datetime(1652786500),
        )
        # Hold-paid STORE captured with the explicit hold payment type
        _add_forgotten_message(
            session,
            item_hash=FORGOTTEN_HOLD_STORE_HASH,
            type=MessageType.store,
            owner=OWNER,
            payment_type="hold",
            size=SMALL_FILE_SIZE,
            forgotten_at=timestamp_to_datetime(1652786500),
        )
        # Row captured before payment_type was coalesced at forget time
        _add_forgotten_message(
            session,
            item_hash=FORGOTTEN_NULL_PAYMENT_STORE_HASH,
            type=MessageType.store,
            owner=OWNER,
            payment_type=None,
            size=SMALL_FILE_SIZE,
            forgotten_at=timestamp_to_datetime(1652786500),
        )
        session.commit()


@pytest.mark.asyncio
async def test_message_price_forgotten_store_with_metadata(
    ccn_api_client,
    session_factory: DbSessionFactory,
    fixture_forgotten_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    A forgotten STORE with preserved metadata is priced live, and the
    MIN_STORE_COST_MIB floor applies: a 1 MiB file is billed as
    MIN_STORE_COST_MIB MiB.
    """
    response = await ccn_api_client.get(PRICE_URI.format(FORGOTTEN_CREDIT_STORE_HASH))
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["payment_type"] == "credit"
    assert data["charged_address"] == OWNER
    assert data["required_tokens"] > 0

    # Expected price: the estimation path for a MIN_STORE_COST_MIB file
    with session_factory() as session:
        expected_cost, _ = get_total_and_detailed_costs(
            session=session,
            content=CostEstimationStoreContent(
                address=OWNER,
                time=1652786100.0,
                item_type=ItemType.storage,
                item_hash=ItemHash(FORGOTTEN_CREDIT_STORE_HASH),
                payment=Payment(type=PaymentType.credit),
                estimated_size_mib=MIN_STORE_COST_MIB,
            ),
            item_hash=FORGOTTEN_CREDIT_STORE_HASH,
        )

    assert data["required_tokens"] == float(expected_cost)
    assert len(data["detail"]) == 1
    assert data["detail"][0]["type"] == "STORAGE"


@pytest.mark.asyncio
async def test_message_price_forgotten_store_include_size(
    ccn_api_client,
    fixture_forgotten_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """include_size returns the preserved (real) size, not the billed floor."""
    response = await ccn_api_client.get(
        PRICE_URI.format(FORGOTTEN_CREDIT_STORE_HASH) + "?include_size=true"
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["detail"][0]["size_mib"] == SMALL_FILE_SIZE / MiB


@pytest.mark.asyncio
async def test_message_price_forgotten_store_without_metadata(
    ccn_api_client,
    fixture_forgotten_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """A forgotten STORE without preserved metadata keeps returning 410 Gone."""
    response = await ccn_api_client.get(PRICE_URI.format(FORGOTTEN_LEGACY_STORE_HASH))
    assert response.status == 410, await response.text()


@pytest.mark.asyncio
async def test_message_price_forgotten_non_store(
    ccn_api_client,
    fixture_forgotten_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Forgotten non-STORE messages keep returning 410 Gone."""
    response = await ccn_api_client.get(PRICE_URI.format(FORGOTTEN_POST_HASH))
    assert response.status == 410, await response.text()


@pytest.mark.asyncio
async def test_message_price_forgotten_hold_store(
    ccn_api_client,
    session_factory: DbSessionFactory,
    fixture_forgotten_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """A forgotten hold-paid STORE is priced as hold (no credit floor)."""
    response = await ccn_api_client.get(PRICE_URI.format(FORGOTTEN_HOLD_STORE_HASH))
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["payment_type"] == "hold"
    assert data["charged_address"] == OWNER

    # Expected price: hold cost of the real (1 MiB) size, floor not applied
    with session_factory() as session:
        expected_cost, _ = get_total_and_detailed_costs(
            session=session,
            content=CostEstimationStoreContent(
                address=OWNER,
                time=1652786100.0,
                item_type=ItemType.storage,
                item_hash=ItemHash(FORGOTTEN_HOLD_STORE_HASH),
                payment=Payment(type=PaymentType.hold),
                estimated_size_mib=SMALL_FILE_SIZE / MiB,
            ),
            item_hash=FORGOTTEN_HOLD_STORE_HASH,
        )

    assert data["required_tokens"] == float(expected_cost)


@pytest.mark.asyncio
async def test_message_price_forgotten_store_null_payment_defaults_to_hold(
    ccn_api_client,
    fixture_forgotten_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    Rows captured before payment_type was coalesced at forget time have NULL
    payment_type: they are priced as hold, matching the live pricing default.
    """
    response = await ccn_api_client.get(
        PRICE_URI.format(FORGOTTEN_NULL_PAYMENT_STORE_HASH)
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["payment_type"] == "hold"
    assert data["charged_address"] == OWNER
    assert data["required_tokens"] > 0
