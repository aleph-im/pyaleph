import datetime as dt
import hashlib
import json
from typing import List, Optional

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from configmanager import Config
from message_test_helpers import process_pending_messages
from sqlalchemy import select

from aleph.db.accessors.credit_ledger import (
    active_attestation,
    apply_confirmation_to_billing,
    get_fold_state,
    record_attestation,
    selected_attestation,
)
from aleph.db.accessors.messages import make_confirmation_upsert_query
from aleph.db.models import ChainTxDb, MessageDb, PendingMessageDb, PostDb
from aleph.db.models.credit_ledger import CreditAttestationDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import MessageStatus

SENDER = "0xAttestor0000000000000000000000000000000001"
PERIOD_START = 1_700_000_400 - (1_700_000_400 % 3600)  # aligned
PERIOD = dt.datetime.fromtimestamp(PERIOD_START, tz=dt.timezone.utc)


@pytest.fixture(autouse=True)
def attestor_config(mock_config: Config) -> None:
    # Must be set before `message_processor` builds its handlers: the handler
    # reads the whitelist at construction.
    mock_config.aleph.credits.attestation.addresses.value = [SENDER]


def _attestation_message(
    *,
    sender: str = SENDER,
    period_start: int = PERIOD_START,
    period_end: Optional[int] = None,
    supersedes: Optional[str] = None,
    nonce: int = 0,
) -> PendingMessageDb:
    if period_end is None:
        period_end = period_start + 3600
    content = {
        "version": 1,
        "period": {"start": period_start, "end": period_end},
        "source": "test-attestor",
        "events_cursor": {"n": nonce},
        "intervals": [
            {
                "vm": "a" * 64,
                "node": "b" * 64,
                "start": period_start,
                "end": period_start + 1800,
            }
        ],
        "totals": {"vms": 1},
    }
    if supersedes is not None:
        content["supersedes"] = supersedes
    item_content = json.dumps(
        {
            "address": sender,
            "time": 1_700_000_000.0 + nonce,
            "type": "aleph-execution-intervals",
            "content": content,
        }
    )
    return PendingMessageDb(
        item_hash=hashlib.sha256(item_content.encode()).hexdigest(),
        type="POST",
        chain="ETH",
        sender=sender,
        signature="0xsig",
        item_type="inline",
        item_content=item_content,
        time=timestamp_to_datetime(1_700_000_000.0 + nonce),
        channel="ALEPH-METERING",
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        check_message=False,
        fetched=True,
        reception_time=dt.datetime(2023, 1, 1),
    )


async def _run(
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
    messages: List[PendingMessageDb],
):
    with session_factory() as session:
        return list(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=messages,
                session=session,
            )
        )


@pytest.mark.asyncio
async def test_valid_attestation_is_recorded_active(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    msg = _attestation_message()
    results = await _run(message_processor, session_factory, [msg])
    assert all(isinstance(r, ProcessedMessage) for r in results)

    with session_factory() as session:
        row = active_attestation(session, PERIOD)
        assert row is not None
        assert row.item_hash == msg.item_hash
        assert row.status == "active"
        assert row.sender == SENDER
        assert row.supersedes is None


@pytest.mark.asyncio
async def test_second_attestation_for_same_period_is_duplicate(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    first = _attestation_message(nonce=1)
    second = _attestation_message(nonce=2)
    await _run(message_processor, session_factory, [first, second])

    with session_factory() as session:
        rows = {
            r.item_hash: r
            for r in session.execute(select(CreditAttestationDb)).scalars()
        }
        assert rows[first.item_hash].status == "active"
        assert rows[second.item_hash].status == "duplicate"
        active = active_attestation(session, PERIOD)
        assert active is not None and active.item_hash == first.item_hash


@pytest.mark.asyncio
async def test_superseding_attestation_is_indexed_and_waits_for_confirmation(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    first = _attestation_message(nonce=1)
    await _run(message_processor, session_factory, [first])

    with session_factory() as session:
        get_fold_state(session).last_closed_period = PERIOD
        session.commit()

    second = _attestation_message(nonce=2, supersedes=first.item_hash)
    await _run(message_processor, session_factory, [second])

    with session_factory() as session:
        rows = {
            r.item_hash: r
            for r in session.execute(select(CreditAttestationDb)).scalars()
        }
        assert rows[first.item_hash].status == "superseded"
        assert rows[second.item_hash].status == "active"
        assert rows[second.item_hash].supersedes == first.item_hash
        active = active_attestation(session, PERIOD)
        assert active is not None and active.item_hash == second.item_hash
        # Status is informational: the unconfirmed post changes nothing the
        # fold reads yet, its confirmation marks the period (tested below).
        assert get_fold_state(session).dirty_from_period is None


@pytest.mark.asyncio
async def test_attestor_address_matches_case_insensitively(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    msg = _attestation_message(sender=SENDER.lower())
    results = await _run(message_processor, session_factory, [msg])
    assert all(isinstance(r, ProcessedMessage) for r in results)

    with session_factory() as session:
        assert session.get(CreditAttestationDb, msg.item_hash) is not None


@pytest.mark.asyncio
async def test_attestation_from_unlisted_address_is_ignored(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    other = "0xSomeoneElse000000000000000000000000000002"
    msg = _attestation_message(sender=other)
    results = await _run(message_processor, session_factory, [msg])
    assert all(isinstance(r, ProcessedMessage) for r in results)

    with session_factory() as session:
        assert session.get(PostDb, msg.item_hash) is not None
        assert list(session.execute(select(CreditAttestationDb)).scalars()) == []


@pytest.mark.asyncio
async def test_malformed_attestation_is_rejected(
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    session_factory: DbSessionFactory,
):
    msg = _attestation_message(period_start=PERIOD_START + 60)
    results = await _run(message_processor, session_factory, [msg])
    assert len(results) == 1
    assert isinstance(results[0], RejectedMessage)

    with session_factory() as session:
        assert list(session.execute(select(CreditAttestationDb)).scalars()) == []


def _record(session, item_hash, supersedes=None):
    return record_attestation(
        session,
        item_hash=item_hash,
        period_start=PERIOD,
        sender=SENDER,
        supersedes=supersedes,
    )


def _statuses(session):
    return {
        r.item_hash: r.status
        for r in session.execute(select(CreditAttestationDb)).scalars()
    }


def test_supersede_chain(session_factory: DbSessionFactory):
    with session_factory() as session:
        assert _record(session, "A") == "active"
        assert _record(session, "B", supersedes="A") == "active"
        assert _record(session, "C", supersedes="A") == "duplicate"
        session.commit()
        assert _statuses(session) == {
            "A": "superseded",
            "B": "active",
            "C": "duplicate",
        }


def test_supersedes_non_active_hash_is_duplicate(session_factory: DbSessionFactory):
    with session_factory() as session:
        assert _record(session, "A") == "active"
        assert _record(session, "B", supersedes="unknown") == "duplicate"
        session.commit()
        assert _statuses(session) == {"A": "active", "B": "duplicate"}


def test_record_attestation_is_idempotent(session_factory: DbSessionFactory):
    with session_factory() as session:
        assert _record(session, "A") == "active"
        assert _record(session, "A") == "active"
        assert _record(session, "B", supersedes="A") == "active"
        assert _record(session, "B", supersedes="A") == "active"
        session.commit()
        assert _statuses(session) == {"A": "superseded", "B": "active"}


def test_supersede_on_open_period_does_not_mark_dirty(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _record(session, "A")
        _record(session, "B", supersedes="A")
        session.commit()
        assert get_fold_state(session).dirty_from_period is None


# --- selection at fold time (spec 7.3) ------------------------------------------


def _message(item_hash: str, confirmed_minutes: Optional[int]) -> MessageDb:
    return MessageDb(
        item_hash=item_hash,
        chain=Chain.ETH,
        sender=SENDER,
        signature="0xsig",
        item_type=ItemType.inline,
        type=MessageType.post,
        content={"address": SENDER, "type": "aleph-execution-intervals"},
        size=100,
        time=PERIOD + dt.timedelta(minutes=61),
        channel="ALEPH-METERING",
        first_confirmed_at=(
            None
            if confirmed_minutes is None
            else PERIOD + dt.timedelta(minutes=confirmed_minutes)
        ),
        status_value=MessageStatus.PROCESSED,
    )


def _receive(session, posts, order):
    """Process the attestation posts in `order`, as a CCN receiving them."""
    for name in order:
        confirmed, supersedes = posts[name]
        session.add(_message(name, confirmed))
        session.flush()
        _record(session, name, supersedes=supersedes)


@pytest.mark.parametrize(
    "order",
    [("A", "B", "C"), ("A", "C", "B"), ("C", "B", "A"), ("B", "C", "A")],
)
def test_supersede_chain_selection_ignores_reception_order(
    session_factory: DbSessionFactory, order
):
    # C supersedes B, which supersedes A; A is the earliest confirmed.
    posts = {"A": (62, None), "B": (70, "A"), "C": (80, "B")}
    with session_factory() as session:
        _receive(session, posts, order)
        assert selected_attestation(session, PERIOD) == "C"
        session.rollback()


@pytest.mark.parametrize("order", [("X", "Y"), ("Y", "X")])
def test_independent_posts_select_the_earliest_confirmed(
    session_factory: DbSessionFactory, order
):
    # Y sorts after X by hash but is confirmed first.
    posts = {"X": (75, None), "Y": (65, None)}
    with session_factory() as session:
        _receive(session, posts, order)
        assert selected_attestation(session, PERIOD) == "Y"
        session.rollback()


def test_equal_confirmations_break_ties_on_item_hash(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _receive(session, {"Y": (65, None), "X": (65, None)}, ("Y", "X"))
        assert selected_attestation(session, PERIOD) == "X"
        session.rollback()


def test_unconfirmed_posts_are_not_received_yet(session_factory: DbSessionFactory):
    with session_factory() as session:
        # The unconfirmed superseder neither counts nor drops its target.
        _receive(session, {"A": (70, None), "B": (None, "A")}, ("A", "B"))
        assert selected_attestation(session, PERIOD) == "A"
        session.rollback()
    with session_factory() as session:
        _receive(session, {"A": (None, None)}, ("A",))
        assert selected_attestation(session, PERIOD) is None
        session.rollback()


def test_late_confirmation_of_a_better_attestation_marks_the_period_dirty(
    session_factory: DbSessionFactory,
):
    tx_hash = "0x" + "c1" * 32
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PERIOD
        _receive(session, {"A": (75, None)}, ("A",))
        state = get_fold_state(session)
        state.dirty_from_period = None
        # Received before its confirmation: no mark, not selected.
        _receive(session, {"B": (None, None)}, ("B",))
        assert get_fold_state(session).dirty_from_period is None
        assert selected_attestation(session, PERIOD) == "A"

        session.add(
            ChainTxDb(
                hash=tx_hash,
                chain=Chain.ETH,
                height=1,
                datetime=PERIOD + dt.timedelta(minutes=62),
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
                protocol_version=1,
                content="x",
            )
        )
        session.flush()
        session.execute(make_confirmation_upsert_query(item_hash="B", tx_hash=tx_hash))
        message_b = session.get(MessageDb, "B")
        assert message_b is not None
        apply_confirmation_to_billing(session, message_b)
        assert selected_attestation(session, PERIOD) == "B"
        assert get_fold_state(session).dirty_from_period == PERIOD
        session.rollback()


def test_attestation_received_already_confirmed_marks_a_closed_period(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PERIOD
        _receive(session, {"A": (75, None)}, ("A",))
        assert get_fold_state(session).dirty_from_period == PERIOD
        session.rollback()
