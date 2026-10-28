"""
A message received over the API or P2P is processed first and confirmed later,
when chain sync replays its tx through MessagePublisher.add_pending_message.
That confirmation must reach the billing hook (apply_confirmation_to_billing)
just like the one going through MessageHandler.confirm_existing_message.
"""

import datetime as dt
import hashlib
import json
from typing import Any, Dict

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from configmanager import Config
from sqlalchemy import update

from aleph.db.accessors.credit_ledger import (
    get_billing_resource,
    get_fold_state,
    record_attestation,
    set_billing_end_of_life,
    upsert_billing_resource,
)
from aleph.db.models import ChainTxDb, MessageDb, MessageStatusDb
from aleph.db.models.credit_ledger import BillingResourceDb
from aleph.handlers.message_handler import MessagePublisher
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageOrigin, MessageStatus

SENDER = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba"
CLOSED = dt.datetime(2026, 3, 1, 12, tzinfo=dt.timezone.utc)
RECEIVED = dt.datetime(2026, 3, 1, 11, tzinfo=dt.timezone.utc)

INSTANCE_CONTENT: Dict[str, Any] = {
    "address": SENDER,
    "time": 1_772_000_000.0,
    "allow_amend": False,
    "payment": {"type": "credit"},
    "environment": {"internet": True, "aleph_api": True},
    "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
    "rootfs": {
        "parent": {"ref": "54" * 32, "use_latest": True},
        "persistence": "host",
        "size_mib": 20 * 1024,
    },
    "volumes": [],
}


@pytest.fixture
def publisher(
    mocker, mock_config: Config, session_factory: DbSessionFactory
) -> MessagePublisher:
    return MessagePublisher(
        session_factory=session_factory,
        storage_service=StorageService(
            storage_engine=mocker.AsyncMock(),
            ipfs_service=mocker.AsyncMock(),
            node_cache=mocker.AsyncMock(),
        ),
        config=mock_config,
        pending_message_exchange=mocker.AsyncMock(),
    )


def _store_processed(
    session: DbSession, type_: MessageType, content: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Store `content` as an already processed inline message and return the
    message dict a chain tx would carry for it.
    """
    item_content = json.dumps(content)
    item_hash = hashlib.sha256(item_content.encode()).hexdigest()
    payment = content.get("payment")
    session.add(
        MessageDb(
            item_hash=item_hash,
            chain=Chain.ETH,
            sender=SENDER,
            signature="0xsig",
            type=type_,
            item_type=ItemType.inline,
            time=dt.datetime.fromtimestamp(content["time"], tz=dt.timezone.utc),
            size=len(item_content),
            content=content,
            owner=SENDER,
            payment_type=payment["type"] if payment else None,
            status_value=MessageStatus.PROCESSED,
            reception_time=RECEIVED,
        )
    )
    session.add(
        MessageStatusDb(
            item_hash=item_hash, status=MessageStatus.PROCESSED, reception_time=RECEIVED
        )
    )
    session.flush()
    return {
        "chain": "ETH",
        "sender": SENDER,
        "type": type_.value,
        "channel": "TEST",
        "signature": "0xsig",
        "time": content["time"],
        "item_type": "inline",
        "item_content": item_content,
        "item_hash": item_hash,
    }


def _chain_tx(session: DbSession, tx_hash: str, at: dt.datetime) -> None:
    session.add(
        ChainTxDb(
            hash=tx_hash,
            chain=Chain.ETH,
            height=1,
            datetime=at,
            publisher="0xabadbabe",
            protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
            protocol_version=1,
            content="x",
        )
    )


def _close_up_to(session: DbSession, period: dt.datetime) -> None:
    state = get_fold_state(session)
    state.last_closed_period = period
    state.dirty_from_period = None


def _dirty(session_factory: DbSessionFactory):
    with session_factory() as session:
        return get_fold_state(session).dirty_from_period


def _resource(session_factory: DbSessionFactory, item_hash: str) -> BillingResourceDb:
    with session_factory() as session:
        row = get_billing_resource(session, item_hash)
        assert row is not None
        return row


async def _confirm(
    publisher: MessagePublisher, message_dict: Dict[str, Any], tx_hash: str
) -> None:
    result = await publisher.add_pending_message(
        message_dict=message_dict,
        reception_time=utc_now(),
        tx_hash=tx_hash,
        origin=MessageOrigin.ONCHAIN,
    )
    assert result is None  # already processed: not queued again


@pytest.mark.asyncio
async def test_chain_confirmation_starts_billing_of_a_processed_instance(
    session_factory: DbSessionFactory, publisher: MessagePublisher
):
    confirmed_at = CLOSED + dt.timedelta(minutes=20)
    tx_hash = "0x" + "a1" * 32
    with session_factory() as session:
        instance = _store_processed(session, MessageType.instance, INSTANCE_CONTENT)
        message = session.get(MessageDb, instance["item_hash"])
        assert message is not None
        upsert_billing_resource(session, message, {"type": "INSTANCE"})
        _chain_tx(session, tx_hash, confirmed_at)
        _close_up_to(session, CLOSED + dt.timedelta(hours=1))
        session.commit()
    assert _resource(session_factory, instance["item_hash"]).first_confirmed_at is None

    await _confirm(publisher, instance, tx_hash)

    row = _resource(session_factory, instance["item_hash"])
    assert row.first_confirmed_at == confirmed_at
    assert _dirty(session_factory) == CLOSED


@pytest.mark.asyncio
async def test_chain_confirmation_finalises_the_end_of_life_of_forget_targets(
    session_factory: DbSessionFactory, publisher: MessagePublisher
):
    confirmed_at = CLOSED + dt.timedelta(minutes=5)
    tx_hash = "0x" + "a2" * 32
    with session_factory() as session:
        instance = _store_processed(session, MessageType.instance, INSTANCE_CONTENT)
        target = instance["item_hash"]
        message = session.get(MessageDb, target)
        assert message is not None
        upsert_billing_resource(session, message, {"type": "INSTANCE"})
        session.execute(
            update(BillingResourceDb)
            .where(BillingResourceDb.item_hash == target)
            .values(first_confirmed_at=RECEIVED)
        )
        # Processed off-chain: the end of life is the reception time until
        # the FORGET is confirmed.
        set_billing_end_of_life(
            session, target, CLOSED + dt.timedelta(minutes=30), "forgotten", True
        )
        forget = _store_processed(
            session,
            MessageType.forget,
            {"address": SENDER, "time": 1.0, "hashes": [target], "reason": "bye"},
        )
        _chain_tx(session, tx_hash, confirmed_at)
        session.commit()

    await _confirm(publisher, forget, tx_hash)

    row = _resource(session_factory, target)
    assert row.end_of_life == confirmed_at
    assert row.end_reason == "forgotten"
    assert row.end_of_life_provisional is False


@pytest.mark.asyncio
async def test_chain_confirmation_of_an_attestation_marks_its_period_dirty(
    session_factory: DbSessionFactory, publisher: MessagePublisher
):
    tx_hash = "0x" + "a3" * 32
    start = int(CLOSED.timestamp())
    content = {
        "address": SENDER,
        "time": 1_772_000_000.0,
        "type": "aleph-execution-intervals",
        "content": {
            "version": 1,
            "period": {"start": start, "end": start + 3600},
            "intervals": [],
        },
    }
    with session_factory() as session:
        post = _store_processed(session, MessageType.post, content)
        record_attestation(session, post["item_hash"], CLOSED, SENDER, None)
        _chain_tx(session, tx_hash, CLOSED + dt.timedelta(hours=2))
        _close_up_to(session, CLOSED + dt.timedelta(hours=1))
        session.commit()

    await _confirm(publisher, post, tx_hash)

    assert _dirty(session_factory) == CLOSED


@pytest.mark.asyncio
async def test_repeated_chain_confirmations_keep_the_earliest(
    session_factory: DbSessionFactory, publisher: MessagePublisher
):
    first_tx, later_tx, earlier_tx = ("0x" + h * 32 for h in ("b1", "b2", "b3"))
    first_at = CLOSED + dt.timedelta(minutes=20)
    with session_factory() as session:
        instance = _store_processed(session, MessageType.instance, INSTANCE_CONTENT)
        message = session.get(MessageDb, instance["item_hash"])
        assert message is not None
        upsert_billing_resource(session, message, {"type": "INSTANCE"})
        _chain_tx(session, first_tx, first_at)
        _chain_tx(session, later_tx, first_at + dt.timedelta(hours=1))
        _chain_tx(session, earlier_tx, CLOSED - dt.timedelta(minutes=50))
        _close_up_to(session, CLOSED + dt.timedelta(hours=1))
        session.commit()
    item_hash = instance["item_hash"]

    await _confirm(publisher, instance, first_tx)
    assert _resource(session_factory, item_hash).first_confirmed_at == first_at

    # The same tx again, then a later one: no error, nothing changes.
    with session_factory() as session:
        _close_up_to(session, CLOSED + dt.timedelta(hours=1))
        session.commit()
    await _confirm(publisher, instance, first_tx)
    await _confirm(publisher, instance, later_tx)
    assert _resource(session_factory, item_hash).first_confirmed_at == first_at
    assert _dirty(session_factory) is None

    # An earlier one lowers the billing start (LEAST) and re-folds its hour.
    await _confirm(publisher, instance, earlier_tx)
    row = _resource(session_factory, item_hash)
    assert row.first_confirmed_at == CLOSED - dt.timedelta(minutes=50)
    assert _dirty(session_factory) == CLOSED - dt.timedelta(hours=1)
