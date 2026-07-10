import datetime as dt
import json

import pytest
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

from aleph.db.accessors.cost import get_message_costs
from aleph.db.accessors.messages import (
    get_message_by_item_hash,
    get_message_status,
    get_rejected_message,
)
from aleph.db.accessors.vms import get_instance
from aleph.db.models import AlephCreditBalanceDb, MessageStatusDb, PendingMessageDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.schemas.api.messages import format_message
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import ErrorCode, MessageStatus

SENDER = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"


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
async def test_process_vprogram(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    user_credit_balance,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    with session_factory() as session:
        status = get_message_status(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert status is not None
        assert status.status == MessageStatus.PROCESSED

        # Costs were persisted for the credit payment.
        costs = list(
            get_message_costs(
                session=session, item_hash=fixture_vprogram_message.item_hash
            )
        )
        assert costs
        assert all(cost.owner == SENDER for cost in costs)

        # Phase 1 keeps no vms side table rows.
        assert (
            get_instance(session=session, item_hash=fixture_vprogram_message.item_hash)
            is None
        )

        # The stored message serializes through the API model.
        message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert message is not None
        formatted = format_message(message)
        assert formatted.type == MessageType.v_program


@pytest.mark.asyncio
async def test_process_vprogram_insufficient_credit(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_vprogram_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # No credit balance is seeded: processing must reject the message.
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    with session_factory() as session:
        status = get_message_status(
            session=session, item_hash=ItemHash(fixture_vprogram_message.item_hash)
        )
        assert status is not None
        assert status.status == MessageStatus.REJECTED

        rejected = get_rejected_message(
            session=session, item_hash=fixture_vprogram_message.item_hash
        )
        assert rejected is not None
        assert rejected.error_code == ErrorCode.CREDIT_INSUFFICIENT
