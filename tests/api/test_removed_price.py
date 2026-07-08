import datetime as dt

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from aleph_message.models.execution.base import Payment, PaymentType

from aleph.db.models.files import StoredFileDb
from aleph.db.models.messages import MessageDb, MessageStatusDb, RemovedMessageDb
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.services.cost import get_total_and_detailed_costs
from aleph.toolkit.constants import MIN_STORE_COST_MIB
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus

PRICE_URI = "/api/v0/price/{}"

OWNER = "0xB6B5358493AF8159B17506C5cC85df69193444BC"

REMOVED_SNAPSHOT_STORE_HASH = (
    "aaaa000000000000000000000000000000000000000000000000000000000000"
)
REMOVED_FALLBACK_STORE_HASH = (
    "bbbb000000000000000000000000000000000000000000000000000000000000"
)
REMOVED_NO_SIZE_STORE_HASH = (
    "cccc000000000000000000000000000000000000000000000000000000000000"
)
REMOVED_POST_HASH = "dddd000000000000000000000000000000000000000000000000000000000000"

FALLBACK_FILE_HASH = "eeee000000000000000000000000000000000000000000000000000000000000"

SMALL_FILE_SIZE = 1024 * 1024  # 1 MiB, well below the MIN_STORE_COST_MIB floor


def _add_removed_message(
    session,
    item_hash: str,
    message_type: MessageType,
    file_hash: str,
) -> None:
    time = timestamp_to_datetime(1652786100)
    content = {
        "address": OWNER,
        "time": 1652786100.0,
    }
    if message_type == MessageType.store:
        content.update(
            {
                "item_hash": file_hash,
                "item_type": ItemType.storage.value,
                "payment": {"type": "credit"},
            }
        )
    else:
        content.update({"type": "test", "content": {"body": "test"}})

    session.add(
        MessageDb(
            item_hash=item_hash,
            sender=OWNER,
            chain=Chain.ETH,
            type=message_type,
            time=time,
            item_type=ItemType.inline,
            signature="sig",
            size=1000,
            content=content,
            status_value=MessageStatus.REMOVED,
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
        # STORE with a removed_messages size snapshot
        _add_removed_message(
            session,
            REMOVED_SNAPSHOT_STORE_HASH,
            MessageType.store,
            file_hash="1111" * 16,
        )
        session.add(
            RemovedMessageDb(
                item_hash=REMOVED_SNAPSHOT_STORE_HASH,
                size=SMALL_FILE_SIZE,
                removed_at=timestamp_to_datetime(1652786500),
            )
        )

        # STORE without snapshot, but the files row still exists (fallback)
        _add_removed_message(
            session,
            REMOVED_FALLBACK_STORE_HASH,
            MessageType.store,
            file_hash=FALLBACK_FILE_HASH,
        )
        session.add(
            StoredFileDb(
                hash=FALLBACK_FILE_HASH, size=SMALL_FILE_SIZE, type=FileType.FILE
            )
        )

        # STORE without snapshot and without files row: size unresolvable
        _add_removed_message(
            session,
            REMOVED_NO_SIZE_STORE_HASH,
            MessageType.store,
            file_hash="2222" * 16,
        )

        # Non-STORE removed message
        _add_removed_message(session, REMOVED_POST_HASH, MessageType.post, file_hash="")

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
    A removed STORE with a size snapshot is priced live, with the
    MIN_STORE_COST_MIB floor applied.
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
async def test_message_price_removed_store_files_fallback(
    ccn_api_client,
    session_factory: DbSessionFactory,
    fixture_removed_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    A removed STORE without a snapshot is priced from the still-existing
    files row.
    """
    response = await ccn_api_client.get(PRICE_URI.format(REMOVED_FALLBACK_STORE_HASH))
    assert response.status == 200, await response.text()
    data = await response.json()

    assert data["payment_type"] == "credit"
    assert data["charged_address"] == OWNER
    assert data["required_tokens"] == _expected_floor_cost(
        session_factory, REMOVED_FALLBACK_STORE_HASH
    )


@pytest.mark.asyncio
async def test_message_price_removed_store_without_size(
    ccn_api_client,
    fixture_removed_messages_for_pricing,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """A removed STORE whose size cannot be resolved keeps returning 410."""
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
