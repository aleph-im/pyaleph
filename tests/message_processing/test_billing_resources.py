import datetime as dt
import json

import pytest
from aleph_message.models import Chain, ItemType, MessageType, PaymentType
from message_test_helpers import process_pending_messages
from sqlalchemy import select

from aleph.db.accessors.credit_ledger import (
    apply_confirmation_to_billing,
    get_billing_resource,
    get_fold_state,
    iter_live_billing_resources,
    refresh_billing_confirmation,
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
from aleph.db.models.messages import ForgottenMessageDb
from aleph.handlers.message_handler import MessageHandler
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
        # Stored in ledger form (EVM lowercased).
        assert row.owner == content["address"].lower()
        assert row.payment_type == PaymentType.credit
        assert row.type == MessageType.instance
        assert row.end_of_life is None
        assert row.cost_spec["type"] == "INSTANCE"
        assert (
            row.cost_spec["content"]["resources"]["vcpus"]
            == content["resources"]["vcpus"]
        )
        assert row.cost_spec["file_size_bytes"] is None
        parent_ref = content["rootfs"]["parent"]["ref"]
        assert row.cost_spec["volume_sizes_bytes"][parent_ref] == 1024 * 1024


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


def _add_resource(
    session,
    item_hash: str,
    type_: MessageType,
    payment_type: PaymentType,
    first_confirmed_at,
    end_of_life=None,
) -> None:
    session.add(
        BillingResourceDb(
            item_hash=item_hash,
            type=type_,
            owner=SENDER,
            payment_type=payment_type,
            first_confirmed_at=first_confirmed_at,
            end_of_life=end_of_life,
            cost_spec={},
        )
    )


def test_iter_live_billing_resources_window(session_factory: DbSessionFactory):
    start = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2026, 3, 2, tzinfo=dt.timezone.utc)
    day = dt.timedelta(days=1)
    credit, hold = PaymentType.credit, PaymentType.hold
    inst, store = MessageType.instance, MessageType.store

    def live(session, type_=None, payment_type=None):
        return {
            r.item_hash
            for r in iter_live_billing_resources(
                session, type_, payment_type, start, end
            )
        }

    with session_factory() as session:
        _add_resource(session, "01", inst, credit, None)  # unconfirmed
        _add_resource(session, "02", inst, credit, end)  # starts at period_end
        _add_resource(session, "03", inst, credit, end - dt.timedelta(seconds=1))
        _add_resource(session, "04", inst, credit, start - day)  # open-ended
        _add_resource(session, "05", inst, credit, start - day, end_of_life=start)
        _add_resource(
            session, "06", inst, credit, start - day, start + dt.timedelta(seconds=1)
        )
        _add_resource(session, "07", store, hold, start - day)
        session.flush()

        assert live(session) == {"03", "04", "06", "07"}
        assert live(session, type_=store) == {"07"}
        assert live(session, type_=inst) == {"03", "04", "06"}
        assert live(session, payment_type=hold) == {"07"}
        assert live(session, inst, hold) == set()


def test_refresh_billing_confirmation_keeps_earliest(
    session_factory: DbSessionFactory,
):
    early = dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc)
    late = dt.datetime(2026, 3, 5, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        _add_resource(session, "aa01", MessageType.instance, PaymentType.credit, None)
        session.flush()

        refresh_billing_confirmation(session, "aa01", late)
        assert _row(session, "aa01").first_confirmed_at == late

        refresh_billing_confirmation(session, "aa01", early)
        session.refresh(_row(session, "aa01"))
        assert _row(session, "aa01").first_confirmed_at == early

        refresh_billing_confirmation(session, "aa01", late)
        session.refresh(_row(session, "aa01"))
        assert _row(session, "aa01").first_confirmed_at == early


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
        assert spec["file_size_bytes"] == 30 * 1024 * 1024
        assert spec["volume_sizes_bytes"] == {}

        upsert_billing_resource(session, message, spec)
        row = get_billing_resource(session, item_hash)
        assert row is not None
        assert row.cost_spec["file_size_bytes"] == 30 * 1024 * 1024
        assert row.type == MessageType.store


@pytest.mark.asyncio
async def test_late_forget_confirmation_replaces_provisional_end_of_life(
    session_factory: DbSessionFactory,
):
    """
    A confirmed duplicate of an already processed FORGET goes through
    MessageHandler.confirm_existing_message and finalises the end of life of
    both its direct target and a target forgotten by cascade.
    """
    target_hash = "ff" * 32
    cascaded_hash = "fe" * 32
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
        session.add(
            ForgottenMessageDb(
                item_hash=cascaded_hash,
                type=MessageType.instance,
                chain=Chain.ETH,
                sender=SENDER,
                signature="sig",
                item_type=ItemType.inline,
                time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                channel=None,
                forgotten_by=[forget_hash],
            )
        )
        _add_resource(
            session, cascaded_hash, MessageType.instance, PaymentType.credit, None
        )
        session.flush()
        upsert_billing_resource(
            session, _get_message(session, target_hash), {"type": "INSTANCE"}
        )
        for h in (target_hash, cascaded_hash):
            set_billing_end_of_life(session, h, received, "forgotten", True)

        pending = PendingMessageDb(
            item_hash=forget_hash,
            type=MessageType.forget,
            chain=Chain.ETH,
            sender=SENDER,
            signature="sig",
            item_type=ItemType.inline,
            time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
            reception_time=received,
            fetched=True,
            check_message=False,
            retries=0,
            next_attempt=dt.datetime(2023, 1, 1),
            tx_hash=tx_hash,
        )
        await MessageHandler.confirm_existing_message(session, forget, pending)

        for h in (target_hash, cascaded_hash):
            row = _row(session, h)
            session.refresh(row)
            assert row.end_of_life == confirmed_at
            assert row.end_of_life_provisional is False


# --- dirty marking of closed periods ------------------------------------------

CLOSED = dt.datetime(2026, 3, 1, 12, tzinfo=dt.timezone.utc)


def _close_up_to(session, period: dt.datetime) -> None:
    get_fold_state(session).last_closed_period = period
    session.flush()


def _dirty(session):
    state = get_fold_state(session)
    session.refresh(state)
    return state.dirty_from_period


def test_lowered_confirmation_marks_closed_period_dirty(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _add_resource(
            session,
            "aa02",
            MessageType.instance,
            PaymentType.credit,
            CLOSED + dt.timedelta(hours=3),
        )
        _close_up_to(session, CLOSED)

        # Later than the existing value: nothing changes, nothing dirty.
        refresh_billing_confirmation(session, "aa02", CLOSED + dt.timedelta(hours=5))
        assert _dirty(session) is None

        # Lowered into a closed hour: that hour is dirty.
        refresh_billing_confirmation(session, "aa02", CLOSED + dt.timedelta(minutes=7))
        assert _dirty(session) == CLOSED


def test_end_of_life_in_closed_period_marks_dirty(session_factory: DbSessionFactory):
    with session_factory() as session:
        _add_resource(session, "aa03", MessageType.store, PaymentType.credit, CLOSED)
        _close_up_to(session, CLOSED + dt.timedelta(hours=1))

        # Provisional end in an open hour: not dirty.
        set_billing_end_of_life(
            session, "aa03", CLOSED + dt.timedelta(hours=2), "forgotten", True
        )
        assert _dirty(session) is None
        # Finalised earlier, in a closed hour: dirty from that hour.
        set_billing_end_of_life(
            session, "aa03", CLOSED + dt.timedelta(minutes=20), "forgotten"
        )
        assert _dirty(session) == CLOSED
        # A final end of life never moves again, so no new mark.
        get_fold_state(session).dirty_from_period = None
        set_billing_end_of_life(session, "aa03", CLOSED - dt.timedelta(hours=5), "x")
        assert _dirty(session) is None


def _chain_tx(tx_hash: str, at: dt.datetime) -> ChainTxDb:
    return ChainTxDb(
        hash=tx_hash,
        chain=Chain.ETH,
        height=1,
        datetime=at,
        publisher="0xabadbabe",
        protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
        protocol_version=1,
        content="x",
    )


def test_earlier_forget_confirmation_lowers_final_end_of_life(
    session_factory: DbSessionFactory,
):
    # Two packing nodes publish the same FORGET: CCNs that see the 12:05 and
    # 12:02 confirmations in either order must converge on 12:02.
    target = "ef" * 32
    forget = MessageDb(
        item_hash="fe" * 32,
        chain=Chain.ETH,
        sender=SENDER,
        signature="sig",
        type=MessageType.forget,
        item_type=ItemType.inline,
        time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        size=10,
        content={"address": SENDER, "time": 1.0, "hashes": [target]},
        status_value=MessageStatus.PROCESSED,
        reception_time=CLOSED,
    )
    late = CLOSED + dt.timedelta(hours=1, minutes=5)
    early = CLOSED + dt.timedelta(minutes=2)
    with session_factory() as session:
        _add_resource(session, target, MessageType.instance, PaymentType.credit, CLOSED)
        session.add(forget)
        session.add(_chain_tx("0x" + "a1" * 32, late))
        session.add(_chain_tx("0x" + "a2" * 32, early))
        session.flush()
        _close_up_to(session, CLOSED + dt.timedelta(hours=2))

        session.execute(
            make_confirmation_upsert_query(
                item_hash=forget.item_hash, tx_hash="0x" + "a1" * 32
            )
        )
        apply_confirmation_to_billing(session, forget)
        row = _row(session, target)
        assert (row.end_of_life, row.end_of_life_provisional) == (late, False)
        assert _dirty(session) == CLOSED + dt.timedelta(hours=1)

        get_fold_state(session).dirty_from_period = None
        session.execute(
            make_confirmation_upsert_query(
                item_hash=forget.item_hash, tx_hash="0x" + "a2" * 32
            )
        )
        apply_confirmation_to_billing(session, forget)
        session.refresh(row)
        assert (row.end_of_life, row.end_reason) == (early, "forgotten")
        assert row.end_of_life_provisional is False
        assert _dirty(session) == CLOSED

        # Never raised again, and a later confirmation marks nothing.
        get_fold_state(session).dirty_from_period = None
        set_billing_end_of_life(session, target, late, "forgotten")
        session.refresh(row)
        assert row.end_of_life == early
        assert _dirty(session) is None


def test_removed_end_of_life_is_never_lowered(session_factory: DbSessionFactory):
    with session_factory() as session:
        _add_resource(session, "aa04", MessageType.store, PaymentType.credit, CLOSED)
        _close_up_to(session, CLOSED + dt.timedelta(hours=2))
        set_billing_end_of_life(
            session, "aa04", CLOSED + dt.timedelta(hours=1), "removed"
        )
        get_fold_state(session).dirty_from_period = None
        set_billing_end_of_life(
            session, "aa04", CLOSED + dt.timedelta(minutes=1), "forgotten"
        )
        row = _row(session, "aa04")
        assert (row.end_of_life, row.end_reason) == (
            CLOSED + dt.timedelta(hours=1),
            "removed",
        )
        assert _dirty(session) is None


def test_removal_in_closed_period_marks_dirty(session_factory: DbSessionFactory):
    item_hash = "cd" * 32
    with session_factory() as session:
        session.add(_instance_message(item_hash))
        session.flush()
        upsert_billing_resource(
            session, _get_message(session, item_hash), {"type": "INSTANCE"}
        )
        _close_up_to(session, CLOSED)
        remove_message(session, item_hash, CLOSED + dt.timedelta(minutes=30))
        assert _dirty(session) == CLOSED


def test_late_transfer_confirmation_marks_dirty(session_factory: DbSessionFactory):
    tx_hash = "0x" + "ce" * 32
    transfer = MessageDb(
        item_hash="ab" * 32,
        chain=Chain.ETH,
        sender=SENDER,
        signature="sig",
        type=MessageType.post,
        item_type=ItemType.inline,
        time=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        size=10,
        content={"address": SENDER, "type": "aleph_credit_transfer", "time": 1.0},
        status_value=MessageStatus.PROCESSED,
        reception_time=CLOSED,
    )
    with session_factory() as session:
        session.add(transfer)
        session.add(
            ChainTxDb(
                hash=tx_hash,
                chain=Chain.ETH,
                height=1,
                datetime=CLOSED + dt.timedelta(minutes=40),
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
                protocol_version=1,
                content="x",
            )
        )
        _close_up_to(session, CLOSED)
        session.execute(
            make_confirmation_upsert_query(
                item_hash=transfer.item_hash, tx_hash=tx_hash
            )
        )
        apply_confirmation_to_billing(session, transfer)
        assert _dirty(session) == CLOSED
