import datetime as dt
import json

import pytest
from aleph_message.models import Chain, ItemType, MessageType, PaymentType
from message_test_helpers import process_pending_messages
from sqlalchemy import select

from aleph.db.accessors.credit_ledger import (
    apply_confirmation_to_billing,
    get_billing_resource,
    iter_live_billing_resources,
    set_billing_end_of_life,
    upsert_billing_resource,
)
from aleph.db.accessors.messages import make_confirmation_upsert_query, remove_message
from aleph.db.models import (
    AlephCreditBalanceDb,
    ChainTxDb,
    MessageDb,
    MessageStatusDb,
    PendingMessageDb,
    StoredFileDb,
)
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.credit_ledger import BillingResourceDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.services.credits.cost_spec import build_cost_spec
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus

SENDER = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"


@pytest.fixture
def credit_instance_message(
    session_factory: DbSessionFactory,
    fixture_instance_message: PendingMessageDb,
    instance_message_with_volumes_in_db,
) -> PendingMessageDb:
    """The standard instance fixture, switched to credit payment and funded."""
    with session_factory() as session:
        pending = session.scalars(
            select(PendingMessageDb).where(
                PendingMessageDb.item_hash == fixture_instance_message.item_hash
            )
        ).one()
        assert pending is not None and pending.item_content
        content = json.loads(pending.item_content)
        content["payment"] = {"type": "credit"}
        pending.item_content = json.dumps(content)
        fixture_instance_message.item_content = pending.item_content
        session.add(
            AlephCreditBalanceDb(
                address=SENDER,
                credit_ref="test-credit-ref",
                credit_index=0,
                amount_remaining=10_000_000_000,
                expiration_date=None,
                message_timestamp=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.commit()
    return fixture_instance_message


async def _process_instance(
    message_processor: PendingMessageProcessor,
) -> None:
    _ = [m async for m in message_processor.make_pipeline()]


@pytest.mark.asyncio
async def test_processing_an_instance_writes_billing_resource(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    credit_instance_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    await _process_instance(message_processor)

    assert credit_instance_message.item_content
    content = json.loads(credit_instance_message.item_content)

    with session_factory() as session:
        row = get_billing_resource(session, credit_instance_message.item_hash)
        assert row is not None
        assert row.owner == content["address"]
        assert row.payment_type == PaymentType.credit
        assert row.type == MessageType.instance
        assert row.end_of_life is None
        assert row.cost_spec["type"] == "INSTANCE"
        assert (
            row.cost_spec["content"]["resources"]["vcpus"]
            == content["resources"]["vcpus"]
        )
        assert row.cost_spec["file_size_mib"] is None
        parent_ref = content["rootfs"]["parent"]["ref"]
        assert row.cost_spec["volume_sizes_mib"][parent_ref] == 1


@pytest.mark.asyncio
async def test_forget_sets_end_of_life_to_forget_confirmation(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    credit_instance_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    await _process_instance(message_processor)
    item_hash = credit_instance_message.item_hash

    confirmed_at = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.timezone.utc)
    tx_hash = "0x" + "ab" * 32
    forget_content = {
        "address": SENDER,
        "time": 1.0,  # sender-controlled and backdated, must be ignored
        "hashes": [item_hash],
        "reason": "bye",
    }
    pending_forget = PendingMessageDb(
        item_hash="8a1497002b2fd19b6036f1ef9a652ad47f1700b3f0d380761dbd347be9178702",
        type=MessageType.forget,
        chain=Chain.ETH,
        sender=SENDER,
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(forget_content),
        time=dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
        channel=None,
        reception_time=dt.datetime(2026, 3, 1, 12, 5, tzinfo=dt.timezone.utc),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        tx_hash=tx_hash,
    )

    with session_factory() as session:
        session.add(
            ChainTxDb(
                hash=tx_hash,
                chain=Chain.ETH,
                height=1234,
                datetime=confirmed_at,
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
                protocol_version=1,
                content="x",
            )
        )
        session.commit()

        assert session.scalars(
            select(AccountCostsDb).where(AccountCostsDb.item_hash == item_hash)
        ).all()

        await process_pending_messages(
            message_processor=message_processor,
            pending_messages=[pending_forget],
            session=session,
        )

        session.expire_all()
        assert (
            session.scalars(
                select(AccountCostsDb).where(AccountCostsDb.item_hash == item_hash)
            ).all()
            == []
        )
        row = get_billing_resource(session, item_hash)
        assert row is not None
        assert row.end_of_life == confirmed_at
        assert row.end_reason == "forgotten"
        assert row.end_of_life_provisional is False


def test_set_billing_end_of_life_provisional_rules(
    session_factory: DbSessionFactory,
):
    item_hash = "aa" * 32
    t1 = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    t2 = dt.datetime(2026, 3, 2, tzinfo=dt.timezone.utc)
    t3 = dt.datetime(2026, 3, 3, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        session.add(_instance_message(item_hash))
        session.flush()
        upsert_billing_resource(
            session, _get_message(session, item_hash), {"type": "INSTANCE"}
        )
        set_billing_end_of_life(session, item_hash, t1, "forgotten", provisional=True)
        row = _row(session, item_hash)
        assert (row.end_of_life, row.end_of_life_provisional) == (t1, True)

        set_billing_end_of_life(session, item_hash, t2, "forgotten", provisional=False)
        session.refresh(row)
        assert (row.end_of_life, row.end_of_life_provisional) == (t2, False)

        set_billing_end_of_life(session, item_hash, t3, "forgotten", provisional=False)
        session.refresh(row)
        assert row.end_of_life == t2


def test_iter_live_billing_resources_window(session_factory: DbSessionFactory):
    item_hash = "bb" * 32
    start = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 3, 2, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        session.add(_instance_message(item_hash))
        session.flush()
        upsert_billing_resource(
            session, _get_message(session, item_hash), {"type": "INSTANCE"}
        )
        # first_confirmed_at is null: excluded
        assert not list(iter_live_billing_resources(session, None, None, start, end))


def _get_message(session, item_hash: str) -> MessageDb:
    message = session.get(MessageDb, item_hash)
    assert message is not None
    return message


def _row(session, item_hash: str) -> BillingResourceDb:
    row = get_billing_resource(session, item_hash)
    assert row is not None
    return row


def _instance_message(item_hash: str) -> MessageDb:
    return MessageDb(
        item_hash=item_hash,
        chain=Chain.ETH,
        sender=SENDER,
        signature="sig",
        type=MessageType.instance,
        item_type=ItemType.inline,
        time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        size=10,
        content={"address": SENDER, "payment": {"type": "credit"}},
        owner=SENDER,
        payment_type="credit",
        status_value=MessageStatus.PROCESSED,
        reception_time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
    )


def test_removal_sets_end_of_life(session_factory: DbSessionFactory):
    item_hash = "cc" * 32
    removed_at = dt.datetime(2026, 4, 1, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        session.add(_instance_message(item_hash))
        session.add(
            MessageStatusDb(
                item_hash=item_hash,
                status=MessageStatus.REMOVING,
                reception_time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.flush()
        upsert_billing_resource(
            session, _get_message(session, item_hash), {"type": "INSTANCE"}
        )
        session.commit()

        remove_message(session, item_hash, removed_at=removed_at)
        session.commit()

        row = get_billing_resource(session, item_hash)
        assert row is not None
        assert row.end_of_life == removed_at
        assert row.end_reason == "removed"
        assert row.end_of_life_provisional is False


def test_store_cost_spec_carries_file_size(session_factory: DbSessionFactory):
    item_hash = "dd" * 32
    file_hash = "Qm" + "e" * 44
    with session_factory() as session:
        session.add(
            StoredFileDb(hash=file_hash, size=30 * 1024 * 1024, type=FileType.FILE)
        )
        message = MessageDb(
            item_hash=item_hash,
            chain=Chain.ETH,
            sender=SENDER,
            signature="sig",
            type=MessageType.store,
            item_type=ItemType.inline,
            time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
            size=10,
            content={
                "address": SENDER,
                "time": 1.0,
                "item_type": "ipfs",
                "item_hash": file_hash,
                "payment": {"type": "credit"},
            },
            owner=SENDER,
            payment_type="credit",
            status_value=MessageStatus.PROCESSED,
            reception_time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        )
        session.add(message)
        session.flush()

        spec = build_cost_spec(session, message)
        assert spec["type"] == "STORE"
        assert spec["file_size_mib"] == 30
        assert spec["volume_sizes_mib"] == {}

        upsert_billing_resource(session, message, spec)
        row = get_billing_resource(session, item_hash)
        assert row is not None
        assert row.cost_spec["file_size_mib"] == 30
        assert row.type == MessageType.store


def test_late_forget_confirmation_replaces_provisional_end_of_life(
    session_factory: DbSessionFactory,
):
    target_hash = "ff" * 32
    forget_hash = "f0" * 32
    received = dt.datetime(2026, 3, 1, 12, 5, tzinfo=dt.timezone.utc)
    confirmed_at = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.timezone.utc)
    tx_hash = "0x" + "cd" * 32
    with session_factory() as session:
        session.add(_instance_message(target_hash))
        forget = MessageDb(
            item_hash=forget_hash,
            chain=Chain.ETH,
            sender=SENDER,
            signature="sig",
            type=MessageType.forget,
            item_type=ItemType.inline,
            time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
            size=10,
            content={"address": SENDER, "hashes": [target_hash], "time": 1.0},
            status_value=MessageStatus.PROCESSED,
            reception_time=received,
        )
        session.add(forget)
        session.add(
            ChainTxDb(
                hash=tx_hash,
                chain=Chain.ETH,
                height=1,
                datetime=confirmed_at,
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
                protocol_version=1,
                content="x",
            )
        )
        session.flush()
        upsert_billing_resource(
            session, _get_message(session, target_hash), {"type": "INSTANCE"}
        )
        set_billing_end_of_life(session, target_hash, received, "forgotten", True)

        session.execute(
            make_confirmation_upsert_query(item_hash=forget_hash, tx_hash=tx_hash)
        )
        session.refresh(forget)
        apply_confirmation_to_billing(session, forget)

        row = _row(session, target_hash)
        session.refresh(row)
        assert row.end_of_life == confirmed_at
        assert row.end_of_life_provisional is False
