import datetime as dt
import logging
import threading
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from aleph_message.models import Chain, ItemType, MessageType
from configmanager import Config
from services.test_credit_inputs import (  # noqa: F401 (pricing is a fixture)
    ATTESTOR,
    NODE_A,
    OWNER_KEY,
    PE,
    PS,
    VM,
    attestation,
    attestation_message,
    interval,
    pricing,
    resource,
    store_spec,
)
from sqlalchemy import select, text, update
from sqlalchemy.exc import OperationalError

from aleph.db.accessors.credit_ledger import (
    get_fold_state,
    mark_dirty,
    record_attestation,
    set_billing_end_of_life,
)
from aleph.db.models import MessageDb, PostDb
from aleph.db.models.credit_ledger import (
    BillingResourceDb,
    CreditAttestationDb,
    CreditCheckpointDb,
    CreditFoldStateDb,
    CreditLedgerDb,
    CreditLotDb,
    CreditPeriodQuoteDb,
)
from aleph.handlers.content.forget import ForgetMessageHandler
from aleph.services.credits.lifecycle import (
    FoldConfig,
    close_ready_periods,
    load_genesis,
    period_is_closable,
    store_period_quote,
    verify_latest_checkpoint,
)
from aleph.toolkit.constants import MiB
from aleph.types.db_session import DbSessionFactory

HOUR = dt.timedelta(hours=1)
GRACE = dt.timedelta(seconds=10800)
GENESIS_HASH = "e" * 64
STORE = "8" * 64


def fold_config(
    start: Optional[dt.datetime] = PS,
    genesis_period: Optional[dt.datetime] = None,
    genesis_hash: Optional[str] = None,
) -> FoldConfig:
    return FoldConfig(
        start_period=start,
        genesis_period=genesis_period,
        genesis_hash=genesis_hash,
        grace_seconds=10800,
        checkpoint_every_hours=24,
    )


def set_cursor(session_factory: DbSessionFactory, time: dt.datetime, block=1000):
    with session_factory() as session:
        state = get_fold_state(session)
        state.chain_cursor_time = time
        state.chain_cursor_block = block
        session.commit()


def ledger(session, period: Optional[dt.datetime] = None) -> List[CreditLedgerDb]:
    stmt = select(CreditLedgerDb).order_by(CreditLedgerDb.id)
    if period is not None:
        stmt = stmt.where(CreditLedgerDb.period == period)
    return list(session.execute(stmt).scalars())


def test_fold_config_reads_unix_seconds(mock_config: Config):
    credits = mock_config.aleph.credits
    credits.attestation.start_period.value = int(PS.timestamp())
    credits.genesis.period.value = int((PS - HOUR).timestamp())
    credits.genesis.hash.value = GENESIS_HASH
    cfg = FoldConfig.from_config(mock_config)
    assert cfg.start_period == PS
    assert cfg.genesis_period == PS - HOUR
    assert cfg.first_period == PS  # the latest of the two
    assert cfg.genesis_hash == GENESIS_HASH
    assert cfg.grace_seconds == 10800
    assert cfg.checkpoint_every_hours == 24


def test_period_not_closable_until_chain_cursor_passes_its_end(
    session_factory: DbSessionFactory,
):
    cfg = fold_config()
    late = PE + GRACE
    set_cursor(session_factory, PE - dt.timedelta(seconds=1))
    with session_factory() as session:
        assert period_is_closable(session, PS, late, cfg) == (False, "chain")

    set_cursor(session_factory, PE)
    with session_factory() as session:
        assert period_is_closable(session, PS, PE + HOUR, cfg) == (
            False,
            "attestation",
        )
        assert period_is_closable(session, PS, late, cfg) == (True, None)
        session.add_all(attestation([]))
        session.flush()
        assert period_is_closable(session, PS, PE, cfg) == (True, None)


def test_unconfirmed_attestation_does_not_make_a_period_closable(
    session_factory: DbSessionFactory,
):
    cfg = fold_config()
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add_all(attestation([], confirmed=None))
        session.flush()
        assert period_is_closable(session, PS, PE, cfg) == (False, "attestation")


@pytest.mark.asyncio
async def test_period_closes_without_attestation_after_grace_with_single_row(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    cfg = fold_config()
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add(resource(VM))
        session.commit()

    # Before the grace: nothing closes.
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 0
    assert await close_ready_periods(session_factory, PE + GRACE, mock_config, cfg) == 1

    with session_factory() as session:
        [row] = ledger(session)
        assert (row.period, row.kind, row.excluded_reason, row.amount) == (
            PS,
            "execution",
            "attestation_missing",
            0,
        )
        assert get_fold_state(session).last_closed_period == PS


@pytest.mark.asyncio
async def test_close_ready_periods_folds_in_order_and_checkpoints_at_midnight(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    midnight = dt.datetime(2025, 6, 2, tzinfo=dt.timezone.utc)
    start = midnight - 2 * HOUR
    cfg = fold_config(start=start)
    set_cursor(session_factory, midnight + HOUR)

    closed = await close_ready_periods(
        session_factory, midnight + HOUR + GRACE, mock_config, cfg
    )

    assert closed == 3
    with session_factory() as session:
        assert [row.period for row in ledger(session)] == [
            start,
            start + HOUR,
            midnight,
        ]
        checkpoint_periods = set(
            session.execute(select(CreditCheckpointDb.period)).scalars()
        )
        assert checkpoint_periods == {midnight}
        assert get_fold_state(session).last_closed_period == midnight


@pytest.mark.asyncio
async def test_nothing_is_folded_without_a_configured_start(
    session_factory: DbSessionFactory, mock_config: Config
):
    set_cursor(session_factory, PE)
    cfg = fold_config(start=None)
    assert await close_ready_periods(session_factory, PE + GRACE, mock_config, cfg) == 0


@pytest.mark.asyncio
async def test_late_forget_marks_dirty_and_refold_shortens_storage(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    cfg = fold_config()
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add(
            resource(STORE, type_=MessageType.store, spec=store_spec(100 * MiB))
        )
        session.add_all(attestation([]))
        session.commit()

    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1
    with session_factory() as session:
        [storage] = [r for r in ledger(session) if r.kind == "storage"]
        assert storage.seconds == 3600

    forget = MessageDb(
        item_hash="f0" * 32,
        chain=Chain.ETH,
        sender=OWNER_KEY,
        signature="sig",
        item_type=ItemType.inline,
        type=MessageType.forget,
        content={"hashes": [STORE]},
        size=10,
        time=PS,
    )
    forget.first_confirmed_at = PS + dt.timedelta(minutes=20)
    with session_factory() as session:
        ForgetMessageHandler._close_billing(session, STORE, forget)
        assert get_fold_state(session).dirty_from_period == PS
        session.commit()

    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1
    with session_factory() as session:
        [storage] = [r for r in ledger(session) if r.kind == "storage"]
        assert storage.seconds == 1200
        state = get_fold_state(session)
        assert state.dirty_from_period is None
        assert state.last_closed_period == PS


@pytest.mark.asyncio
async def test_late_attestation_for_missing_period_refolds(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    cfg = fold_config()
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add(resource(VM))
        session.commit()

    assert await close_ready_periods(session_factory, PE + GRACE, mock_config, cfg) == 1

    message, post, _ = attestation([interval(VM, NODE_A, 0, 60)])
    with session_factory() as session:
        session.add_all([message, post])
        session.flush()
        status = record_attestation(session, post.item_hash, PS, ATTESTOR, None)
        assert status == "active"
        assert get_fold_state(session).dirty_from_period == PS
        session.commit()

    assert await close_ready_periods(session_factory, PE + GRACE, mock_config, cfg) == 1
    with session_factory() as session:
        rows = ledger(session, PS)
        assert [r.excluded_reason for r in rows] == [None]
        [execution] = rows
        assert (execution.kind, execution.resource, execution.node_id) == (
            "execution",
            VM,
            NODE_A,
        )
        assert execution.seconds == 3600


class FakeStorage:
    def __init__(self, entries: List[Dict[str, Any]]):
        self.entries = entries
        self.calls: List[str] = []

    async def get_json(self, content_hash: str, **kwargs):
        self.calls.append(content_hash)
        return SimpleNamespace(value=self.entries)


GENESIS_ENTRIES = [
    {
        "address": "0xAbCdEf0000000000000000000000000000000001",
        "amount": 1_000,
        "expiration": None,
        "ref": "lot-a",
    },
    {
        "address": "0xAbCdEf0000000000000000000000000000000002",
        "amount": 250,
        "expiration": int((PS + dt.timedelta(days=30)).timestamp()),
        "ref": "lot-b",
    },
]


def lots(session):
    return [
        (lot.address, lot.amount_remaining, lot.expiration, lot.business_key)
        for lot in session.execute(
            select(CreditLotDb).order_by(CreditLotDb.business_key)
        ).scalars()
    ]


@pytest.mark.asyncio
async def test_load_genesis_is_idempotent_and_creates_lots(
    session_factory: DbSessionFactory,
):
    storage = FakeStorage(GENESIS_ENTRIES)
    with session_factory() as session:
        assert await load_genesis(session, storage, GENESIS_HASH, PS) == 2
        session.commit()
    with session_factory() as session:
        assert await load_genesis(session, storage, GENESIS_HASH, PS) == 0
        session.commit()

    with session_factory() as session:
        assert lots(session) == [
            (
                "0xabcdef0000000000000000000000000000000001",
                1_000,
                None,
                f"{GENESIS_HASH}:0",
            ),
            (
                "0xabcdef0000000000000000000000000000000002",
                250,
                PS + dt.timedelta(days=30),
                f"{GENESIS_HASH}:1",
            ),
        ]
        rows = ledger(session)
        assert {(r.kind, r.period, r.source_ref) for r in rows} == {
            ("genesis", PS, "lot-a"),
            ("genesis", PS, "lot-b"),
        }
        state = get_fold_state(session)
        assert state.genesis_loaded_hash == GENESIS_HASH
        assert state.dirty_from_period is None


@pytest.mark.asyncio
async def test_late_genesis_marks_dirty_and_survives_the_refold(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    """
    Genesis rows are not period inputs: a refold over the genesis period must
    carry them over, ahead of the period's other rows.
    """
    cfg = fold_config(genesis_period=PS, genesis_hash=GENESIS_HASH)
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add(
            resource(STORE, type_=MessageType.store, spec=store_spec(100 * MiB))
        )
        session.add_all(attestation([]))
        session.commit()
    # The fold waits for the configured genesis.
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 0
    with session_factory() as session:
        # Loaded as repair_node does at startup.
        await load_genesis(session, FakeStorage(GENESIS_ENTRIES), GENESIS_HASH, PS)
        session.commit()
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1

    # A later input for the genesis period refolds it.
    with session_factory() as session:
        set_billing_end_of_life(
            session, STORE, PS + dt.timedelta(minutes=30), "removed"
        )
        assert get_fold_state(session).dirty_from_period == PS
        session.commit()
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1

    with session_factory() as session:
        kinds = [r.kind for r in ledger(session, PS)]
        assert kinds == ["genesis", "genesis", "storage"]
        [storage] = [r for r in ledger(session) if r.kind == "storage"]
        assert storage.seconds == 1800


@pytest.mark.asyncio
async def test_a_different_genesis_hash_is_refused(
    session_factory: DbSessionFactory, caplog
):
    with session_factory() as session:
        await load_genesis(session, FakeStorage(GENESIS_ENTRIES), GENESIS_HASH, PS)
        session.commit()
    with session_factory() as session:
        before = [(r.kind, r.business_key, r.amount) for r in ledger(session)]

    other = "d" * 64
    caplog.set_level(logging.ERROR, logger="aleph.services.credits.lifecycle")
    with session_factory() as session:
        assert await load_genesis(session, FakeStorage(GENESIS_ENTRIES), other, PS) == 0
        session.commit()

    with session_factory() as session:
        assert [(r.kind, r.business_key, r.amount) for r in ledger(session)] == before
        assert len(lots(session)) == 2
        assert get_fold_state(session).genesis_loaded_hash == GENESIS_HASH
    [record] = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert GENESIS_HASH in record.getMessage() and other in record.getMessage()


@pytest.mark.asyncio
async def test_fold_transaction_locks_the_fold_state_before_reading_inputs(
    session_factory: DbSessionFactory,
    mock_config: Config,
    pricing,  # noqa: F811
    monkeypatch,
):
    """
    While a period is being folded, a concurrent dirty mark must wait: probe
    from another session with NOWAIT while collect_inputs runs.
    """
    from aleph.services.credits import lifecycle

    cfg = fold_config()
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add_all(attestation([]))
        session.commit()

    probes: List[bool] = []
    real_collect = lifecycle.collect_inputs  # type: ignore[attr-defined]

    def collect(session, period, settings, **kwargs):
        with session_factory() as other:
            try:
                other.execute(
                    select(CreditFoldStateDb).with_for_update(nowait=True)
                ).all()
                probes.append(False)
            except OperationalError:
                probes.append(True)
        return real_collect(session, period, settings, **kwargs)

    monkeypatch.setattr(lifecycle, "collect_inputs", collect)
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1
    assert probes == [True]


def test_mark_dirty_locks_the_fold_state(session_factory: DbSessionFactory):
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PS
        session.commit()
    with session_factory() as session:
        assert mark_dirty(session, PS)
        with session_factory() as other:
            with pytest.raises(OperationalError):
                other.execute(
                    select(CreditFoldStateDb).with_for_update(nowait=True)
                ).all()
        session.commit()


def test_mark_dirty_waits_for_an_in_flight_fold_whatever_the_period(
    session_factory: DbSessionFactory,
):
    """
    No lock-free fast path: a fold may close PS + 1h, then PS + 2h, while a
    message transaction that wrote an input for PS + 2h is still open, so even
    a period well past the closed mark waits for the fold transaction.
    """
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PS
        session.commit()
    with session_factory() as holder:
        get_fold_state(holder, for_update=True)
        for period in (PS, PS + HOUR, PS + 2 * HOUR, PS + 5 * HOUR):
            with session_factory() as session:
                session.execute(text("SET LOCAL lock_timeout = '200ms'"))
                with pytest.raises(OperationalError):
                    mark_dirty(session, period)
        holder.rollback()
    with session_factory() as session:
        assert mark_dirty(session, PS + 2 * HOUR) is False
        with session_factory() as other:
            # Other message transactions are not blocked...
            other.execute(
                select(CreditFoldStateDb).with_for_update(
                    read=True, key_share=True, nowait=True
                )
            ).all()
            # ...but a fold transaction waits for this one to end.
            with pytest.raises(OperationalError):
                other.execute(
                    select(CreditFoldStateDb).with_for_update(nowait=True)
                ).all()
        session.commit()
    with session_factory() as session:
        assert get_fold_state(session).dirty_from_period is None


def test_mark_dirty_before_any_close_waits_for_the_first_close(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        assert get_fold_state(session).last_closed_period is None
        assert mark_dirty(session, PS) is False
        with session_factory() as other:
            # Other message transactions are not blocked...
            other.execute(
                select(CreditFoldStateDb).with_for_update(
                    read=True, key_share=True, nowait=True
                )
            ).all()
            # ...but a fold transaction waits for this one to commit.
            with pytest.raises(OperationalError):
                other.execute(
                    select(CreditFoldStateDb).with_for_update(nowait=True)
                ).all()
        session.commit()


def test_concurrent_marks_of_the_same_period_do_not_deadlock(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PS + HOUR
        session.commit()
    results: List[Any] = []

    with session_factory() as first, session_factory() as second:
        # Both hold the shared lock before either marks.
        assert mark_dirty(first, PS + 5 * HOUR) is False
        assert mark_dirty(second, PS + 5 * HOUR) is False
        assert mark_dirty(first, PS + HOUR) is True

        def mark_second():
            try:
                results.append(mark_dirty(second, PS))
                second.commit()
            except Exception as error:  # pragma: no cover - reported below
                results.append(error)

        thread = threading.Thread(target=mark_second)
        thread.start()
        # The second mark waits for the first transaction's row update...
        thread.join(0.5)
        assert thread.is_alive()
        first.commit()
        # ...and goes through as soon as it commits.
        thread.join(10)
        assert not thread.is_alive()
    assert results == [True]
    with session_factory() as session:
        assert get_fold_state(session).dirty_from_period == PS


def test_fold_waits_for_a_message_transaction_and_sees_its_mark(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PS + HOUR
        session.commit()
    with session_factory() as writer:
        assert mark_dirty(writer, PS + HOUR) is True
        with session_factory() as fold:
            fold.execute(text("SET LOCAL lock_timeout = '200ms'"))
            with pytest.raises(OperationalError):
                get_fold_state(fold, for_update=True)
        writer.commit()
    with session_factory() as fold:
        fold.execute(text("SET LOCAL lock_timeout = '200ms'"))
        state = get_fold_state(fold, for_update=True)
        assert state.dirty_from_period == PS + HOUR
        fold.commit()


def test_mark_dirty_keeps_a_loaded_fold_state_in_sync(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PS + HOUR
        session.commit()
    with session_factory() as session:
        state = get_fold_state(session, for_update=True)
        state.chain_cursor_block = 42
        assert mark_dirty(session, PS + HOUR)
        assert mark_dirty(session, PS)
        assert mark_dirty(session, PS + HOUR)
        assert state.dirty_from_period == PS
        session.commit()
    with session_factory() as session:
        state = get_fold_state(session)
        assert (state.dirty_from_period, state.chain_cursor_block) == (PS, 42)


@pytest.mark.asyncio
async def test_genesis_loaded_after_close_marks_dirty(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    cfg = fold_config()
    set_cursor(session_factory, PE)
    with session_factory() as session:
        session.add_all(attestation([]))
        session.commit()
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1

    with session_factory() as session:
        assert await load_genesis(session, FakeStorage(GENESIS_ENTRIES), "g", PS) == 2
        assert get_fold_state(session).dirty_from_period == PS
        session.commit()

    cfg = fold_config(genesis_period=PS, genesis_hash="g")
    assert await close_ready_periods(session_factory, PE, mock_config, cfg) == 1
    with session_factory() as session:
        assert [r.kind for r in ledger(session, PS)] == ["genesis", "genesis"]
        assert len(lots(session)) == 2


def fake_indexer(mocker, blocks: Dict[int, dt.datetime], quote=(5, 10**14)):
    async def get_block(number):
        if number not in blocks:
            raise ValueError(f"unknown block {number}")
        return {"number": number, "timestamp": int(blocks[number].timestamp())}

    indexer = mocker.Mock()
    indexer.cfg = SimpleNamespace(aleph="0xALEPH")
    indexer.quote = mocker.AsyncMock(return_value=quote)
    indexer.web3.eth.get_block = mocker.AsyncMock(side_effect=get_block)
    return indexer


@pytest.mark.asyncio
async def test_period_quote_uses_the_cursor_block_when_it_ends_the_period(
    session_factory: DbSessionFactory, mocker
):
    set_cursor(session_factory, PE, block=1000)
    indexer = fake_indexer(mocker, {})
    with session_factory() as session:
        await store_period_quote(session, PS, indexer)
        session.commit()

    indexer.quote.assert_awaited_once_with("0xALEPH", 10**18, 1000, apply_bonus=False)
    with session_factory() as session:
        quote = session.get(CreditPeriodQuoteDb, PS)
        assert quote is not None
        # 5e-14 credits per raw unit: 50_000 credits per ALEPH.
        assert quote.aleph_per_credit == Decimal("0.00002")
        assert quote.block_number == 1000


@pytest.mark.asyncio
async def test_period_quote_walks_back_to_the_last_block_of_the_period(
    session_factory: DbSessionFactory, mocker
):
    # Blocks every 12 s up to the period end, then every other slot missed.
    blocks = {n: PE - dt.timedelta(seconds=12 * (101 - n)) for n in range(0, 100)}
    blocks.update(
        {n: PE + dt.timedelta(seconds=24 * (n - 99)) for n in range(100, 140)}
    )
    set_cursor(session_factory, blocks[139], block=139)
    indexer = fake_indexer(mocker, blocks)
    with session_factory() as session:
        await store_period_quote(session, PS, indexer)
        session.commit()

    # Block 99 is at PE - 24 s, block 100 at PE + 24 s.
    indexer.quote.assert_awaited_once_with("0xALEPH", 10**18, 99, apply_bonus=False)


@pytest.mark.asyncio
async def test_period_quote_rpc_failure_stores_nothing(
    session_factory: DbSessionFactory, mocker
):
    set_cursor(session_factory, PE + HOUR, block=1000)
    indexer = fake_indexer(mocker, {})
    with session_factory() as session:
        await store_period_quote(session, PS, indexer)
        session.commit()
    with session_factory() as session:
        assert session.get(CreditPeriodQuoteDb, PS) is None


@pytest.mark.asyncio
async def test_verify_latest_checkpoint_marks_dirty_on_mismatch(
    session_factory: DbSessionFactory, mock_config: Config, pricing  # noqa: F811
):
    midnight = dt.datetime(2025, 6, 2, tzinfo=dt.timezone.utc)
    cfg = fold_config(start=midnight - HOUR)
    set_cursor(session_factory, midnight + HOUR)
    with session_factory() as session:
        session.add(
            resource(STORE, type_=MessageType.store, spec=store_spec(100 * MiB))
        )
        session.commit()
    await close_ready_periods(
        session_factory, midnight + HOUR + GRACE, mock_config, cfg
    )

    assert verify_latest_checkpoint(session_factory, mock_config, cfg) is True
    with session_factory() as session:
        assert get_fold_state(session).dirty_from_period is None

    # An input changed without a dirty mark (the hooks bypassed): the re-fold
    # no longer matches what was folded.
    with session_factory() as session:
        session.execute(
            update(BillingResourceDb)
            .where(BillingResourceDb.item_hash == STORE)
            .values(end_of_life=midnight + dt.timedelta(minutes=10))
        )
        session.commit()
    assert verify_latest_checkpoint(session_factory, mock_config, cfg) is False
    with session_factory() as session:
        state = get_fold_state(session)
        assert state.dirty_from_period == midnight - HOUR
        # Verification itself changed nothing else.
        assert state.last_closed_period == midnight
        storage = [r.seconds for r in ledger(session) if r.kind == "storage"]
        assert storage == [3600, 3600]


def test_record_attestation_marks_a_closed_missing_period_dirty(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        get_fold_state(session).last_closed_period = PS
        session.add(attestation_message("a" * 64))
        session.add(
            PostDb(
                item_hash="a" * 64,
                owner=ATTESTOR,
                type="aleph-execution-intervals",
                content={},
                creation_datetime=PE,
            )
        )
        session.flush()
        assert record_attestation(session, "a" * 64, PS, ATTESTOR, None) == "active"
        assert get_fold_state(session).dirty_from_period == PS
        assert session.get(CreditAttestationDb, "a" * 64) is not None


def test_unquoted_closed_periods_are_bounded_to_the_window(
    session_factory: DbSessionFactory,
):
    from aleph.services.credits.lifecycle import (
        REQUOTE_WINDOW_PERIODS,
        unquoted_closed_periods,
    )

    first = PS - 200 * HOUR
    with session_factory() as session:
        session.add(
            CreditPeriodQuoteDb(
                period=PS - HOUR, aleph_per_credit=Decimal(1), block_number=1
            )
        )
        session.flush()
        missing = unquoted_closed_periods(session, first, PS)
        # Never before the fold's first period.
        assert unquoted_closed_periods(session, PS - 2 * HOUR, PS) == [
            PS - 2 * HOUR,
            PS,
        ]

    assert len(missing) == REQUOTE_WINDOW_PERIODS - 1
    assert missing[0] == PS - (REQUOTE_WINDOW_PERIODS - 1) * HOUR
    assert missing[-1] == PS
    assert PS - HOUR not in missing
