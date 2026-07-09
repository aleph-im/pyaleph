import datetime as dt
from typing import Optional

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from aleph_message.models.execution.base import Payment, PaymentType

from aleph.db.models.messages import MessageStatusDb, RemovedMessageDb
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.services.cost import get_total_and_detailed_costs
from aleph.toolkit.constants import MIN_STORE_COST_MIB
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

PRICE_URI = "/api/v0/price/{}"

OWNER = "0xB6B5358493AF8159B17506C5cC85df69193444BC"

REMOVED_SNAPSHOT_STORE_HASH = (
    "aaaa000000000000000000000000000000000000000000000000000000000000"
)
REMOVED_NULL_PAYMENT_STORE_HASH = (
    "bbbb000000000000000000000000000000000000000000000000000000000000"
)
REMOVED_NO_SIZE_STORE_HASH = (
    "cccc000000000000000000000000000000000000000000000000000000000000"
)
REMOVED_POST_HASH = "dddd000000000000000000000000000000000000000000000000000000000000"

SMALL_FILE_SIZE = 1024 * 1024  # 1 MiB, well below the MIN_STORE_COST_MIB floor


def _add_removed_snapshot(
    session,
    item_hash: str,
    message_type: MessageType,
    payment_type: Optional[str],
    size: Optional[int],
) -> None:
    session.add(
        RemovedMessageDb(
            item_hash=item_hash,
            type=message_type,
            chain=Chain.ETH,
            sender=OWNER,
            signature="sig",
            item_type=ItemType.inline,
            time=timestamp_to_datetime(1652786100),
            owner=OWNER,
            payment_type=payment_type,
            size=size,
            removed_at=timestamp_to_datetime(1652786500),
        )
    )
    session.add(
        MessageStatusDb(
            item_hash=item_hash,
            status=MessageStatus.REMOVED,
            reception_time=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
        )
    )


@pytest.fixture
def fixture_removed_messages_for_pricing(session_factory: DbSessionFactory):
    with session_factory() as session:
        # STORE snapshot with full billing metadata
        _add_removed_snapshot(
            session,
            REMOVED_SNAPSHOT_STORE_HASH,
            MessageType.store,
            payment_type="credit",
            size=SMALL_FILE_SIZE,
        )

        # Legacy row moved before payment_type coalescing: NULL means hold
        _add_removed_snapshot(
            session,
            REMOVED_NULL_PAYMENT_STORE_HASH,
            MessageType.store,
            payment_type=None,
            size=SMALL_FILE_SIZE,
        )

        # STORE whose size was never snapshotted: unresolvable
        _add_removed_snapshot(
            session,
            REMOVED_NO_SIZE_STORE_HASH,
            MessageType.store,
            payment_type="credit",
            size=None,
        )

        # Non-STORE removed message
        _add_removed_snapshot(
            session,
            REMOVED_POST_HASH,
            MessageType.post,
            payment_type="hold",
            size=None,
        )

        session.commit()


def _expected_floor_cost(session_factory: DbSessionFactory, item_hash: str) -> float:
    with session_factory() as session:
        expected_cost, _ = get_total_and_detailed_costs(
            session=session,
            content=CostEstimationStoreContent(
                address=OWNER,
                time=1652786100.0,
                item_type=ItemType.storage,
                item_hash=ItemHash(item_hash),
                payment=Payment(type=PaymentType.credit),
                estimated_size_mib=MIN_STORE_COST_MIB,
            ),
            item_hash=item_hash,
        )
    return float(expected_cost)


@pytest.mark.asyncio
async def test_message_price_removed_store_with_snapshot(
    ccn_api_client,
    session_factory: DbSessionFactory,
    fixture_removed_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    A removed STORE snapshot is priced live, with the MIN_STORE_COST_MIB
    floor applied.
    """
    response = await ccn_api_client.get(PRICE_URI.format(REMOVED_SNAPSHOT_STORE_HASH))
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["payment_type"] == "credit"
    assert data["charged_address"] == OWNER
    assert data["required_tokens"] == _expected_floor_cost(
        session_factory, REMOVED_SNAPSHOT_STORE_HASH
    )


@pytest.mark.asyncio
async def test_message_price_removed_store_null_payment_defaults_to_hold(
    ccn_api_client,
    fixture_removed_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    Legacy snapshots with NULL payment_type are priced as hold, matching the
    live pricing default (absence of a payment field means hold).
    """
    response = await ccn_api_client.get(
        PRICE_URI.format(REMOVED_NULL_PAYMENT_STORE_HASH)
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["payment_type"] == "hold"
    assert data["charged_address"] == OWNER
    assert data["required_tokens"] > 0


@pytest.mark.asyncio
async def test_message_price_removed_store_without_size(
    ccn_api_client,
    fixture_removed_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """A removed STORE whose size was never snapshotted returns 410."""
    response = await ccn_api_client.get(PRICE_URI.format(REMOVED_NO_SIZE_STORE_HASH))
    assert response.status == 410, await response.text()


@pytest.mark.asyncio
async def test_message_price_removed_non_store(
    ccn_api_client,
    fixture_removed_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Removed non-STORE messages keep returning 410 Gone."""
    response = await ccn_api_client.get(PRICE_URI.format(REMOVED_POST_HASH))
    assert response.status == 410, await response.text()
