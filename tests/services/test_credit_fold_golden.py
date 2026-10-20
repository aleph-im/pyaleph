"""
Golden fold (spec 9): fixed inputs covering every row kind fold to literal
hashes. A change to the fold, the row hash tuple or the checkpoint hash chain
changes these strings and must be a deliberate, versioned decision: every CCN
has to produce the same hashes from the same inputs.
"""

import datetime as dt
from decimal import Decimal
from typing import Any, List, Tuple

from sqlalchemy import select

from aleph.db.models.credit_ledger import (
    CreditCheckpointDb,
    CreditLedgerDb,
    CreditLotDb,
)
from aleph.services.credits.fold import (
    ExclusionInput,
    ExecutionInput,
    PeriodInputs,
    StorageInput,
    TopupInput,
    TransferInput,
    apply_period,
    period_hash,
    refold_from,
    write_checkpoint,
)
from aleph.types.db_session import DbSessionFactory

# A checkpoint period (hour 0 of a day), so the global hash is written.
PERIOD = dt.datetime(2026, 10, 20, tzinfo=dt.timezone.utc)
UNIX = int(PERIOD.timestamp())
ALICE = "0x00000000000000000000000000000000000a11ce"
BOB = "0x0000000000000000000000000000000000000b0b"
CAROL = "0x00000000000000000000000000000000000ca201"
VM = "1" * 64
HOLD_VM = "2" * 64
NODE = "a" * 64
TRANSFER_1 = "7" * 64
TRANSFER_2 = "8" * 64
EXPIRES = dt.datetime(2027, 1, 1, tzinfo=dt.timezone.utc)

GOLDEN_PERIOD_HASH = "8355648fb2b26297b96957a027561ef6c3d1c9ee3ad43f25493890f83efeb805"
GOLDEN_GLOBAL_HASH = "9f62e9d1d18a06edc67ce262aa35d95496ac044b679f8c1fa3a1b8a546cf0713"


def golden_inputs(reverse: bool = False) -> PeriodInputs:
    topups = [
        TopupInput(
            address=ALICE,
            credits=1_000_000,
            business_key="1:0x" + "ab" * 32 + ":3",
            source_ref="0x" + "ab" * 32,
        )
    ]
    # Applied in this order (confirmation then hash): the second transfer
    # spends credits BOB only holds from the first.
    transfers = [
        TransferInput(
            message_hash=TRANSFER_1,
            sender=ALICE,
            entries=[(BOB, 300_000, EXPIRES)],
        ),
        TransferInput(
            message_hash=TRANSFER_2,
            sender=BOB,
            entries=[(CAROL, 100_000, None)],
        ),
    ]
    executions = [
        ExecutionInput(
            vm=VM,
            node=NODE,
            owner=ALICE,
            payment_type="credit",
            seconds=3600,
            rate=Decimal("0.0125"),
        ),
        ExecutionInput(
            vm=HOLD_VM,
            node=NODE,
            owner=CAROL,
            payment_type="hold",
            seconds=1800,
            rate=Decimal("0.003333333333333333"),
        ),
    ]
    storage = [
        StorageInput(address=BOB, amount=1234, count=2, size_mib=300, seconds=3600)
    ]
    exclusions = [
        ExclusionInput(
            kind="execution",
            business_key=f"{UNIX}:{'3' * 64}:{NODE}",
            address=ALICE,
            resource="3" * 64,
            node_id=NODE,
            reason="payment_stream",
        ),
        ExclusionInput(
            kind="storage",
            business_key=f"{UNIX}:{'4' * 64}",
            address=CAROL,
            resource="4" * 64,
            node_id=None,
            reason="unpriceable",
        ),
    ]
    if reverse:
        topups.reverse()
        executions.reverse()
        storage.reverse()
        exclusions.reverse()
    return PeriodInputs(
        topups=topups,
        transfers=transfers,
        executions=executions,
        storage=storage,
        exclusions=exclusions,
    )


def ledger_snapshot(session) -> List[Tuple[Any, ...]]:
    return [
        (
            r.period,
            r.address,
            r.kind,
            r.business_key,
            r.amount,
            r.resource,
            r.node_id,
            r.seconds,
            r.rate,
            r.expiration,
            r.excluded_reason,
            r.source_ref,
        )
        for r in session.execute(
            select(CreditLedgerDb).order_by(
                CreditLedgerDb.kind, CreditLedgerDb.business_key
            )
        ).scalars()
    ]


def lots_snapshot(session) -> List[Tuple[Any, ...]]:
    return [
        (lot.address, lot.kind, lot.business_key, lot.amount_remaining, lot.expiration)
        for lot in session.execute(
            select(CreditLotDb).order_by(CreditLotDb.kind, CreditLotDb.business_key)
        ).scalars()
    ]


def checkpoint_snapshot(session) -> List[Tuple[Any, ...]]:
    return [
        (c.address, c.ledger_hash, [lot["amount_remaining"] for lot in c.lots])
        for c in session.execute(
            select(CreditCheckpointDb).order_by(CreditCheckpointDb.address)
        ).scalars()
    ]


def test_golden_fold_hashes(session_factory: DbSessionFactory):
    with session_factory() as session:
        result = apply_period(session, PERIOD, golden_inputs())
        global_hash = write_checkpoint(session, PERIOD)

        balances = {
            address: sum(
                amount
                for owner, _, _, amount, _ in lots_snapshot(session)
                if owner == address
            )
            for address in (ALICE, BOB, CAROL)
        }
        # 1_000_000 - 300_000 - 45 (3600 s at 0.0125); BOB 300_000 - 100_000
        # - 1234; CAROL 100_000 (hold_equivalent does not drain).
        assert balances == {ALICE: 699_955, BOB: 198_766, CAROL: 100_000}
        kinds = sorted(row[2] for row in ledger_snapshot(session))
        assert kinds == [
            "execution",
            "execution",
            "hold_equivalent",
            "storage",
            "storage",
            "topup",
            "transfer_in",
            "transfer_in",
            "transfer_out",
            "transfer_out",
        ]

        assert result.period_hash == period_hash(session, PERIOD)
        assert result.period_hash == GOLDEN_PERIOD_HASH
        assert global_hash == GOLDEN_GLOBAL_HASH
        session.rollback()


def test_refold_with_reordered_inputs_is_identical(session_factory: DbSessionFactory):
    with session_factory() as session:
        first = apply_period(session, PERIOD, golden_inputs())
        first_global = write_checkpoint(session, PERIOD)
        rows, lots, checkpoints = (
            ledger_snapshot(session),
            lots_snapshot(session),
            checkpoint_snapshot(session),
        )

        refold_from(session, PERIOD)
        assert ledger_snapshot(session) == []
        assert lots_snapshot(session) == []

        second = apply_period(session, PERIOD, golden_inputs(reverse=True))
        second_global = write_checkpoint(session, PERIOD)

        assert ledger_snapshot(session) == rows
        assert lots_snapshot(session) == lots
        assert checkpoint_snapshot(session) == checkpoints
        assert second.period_hash == first.period_hash
        assert second_global == first_global
        session.rollback()


def test_period_hash_covers_rate_and_expiration(session_factory: DbSessionFactory):
    def fold(rate: Decimal, expiration) -> str:
        with session_factory() as session:
            inputs = PeriodInputs(
                topups=[
                    TopupInput(
                        address=ALICE,
                        credits=100,
                        business_key="1:0xt:0",
                        source_ref=None,
                        expiration=expiration,
                    )
                ],
                executions=[
                    ExecutionInput(
                        vm=VM,
                        node=NODE,
                        owner=ALICE,
                        payment_type="credit",
                        seconds=10,
                        rate=rate,
                    )
                ],
            )
            result = apply_period(session, PERIOD, inputs)
            session.rollback()
        return result.period_hash

    # Same amounts (ceil(10 * rate) == 10), different rate or expiration.
    base = fold(Decimal(1), None)
    assert fold(Decimal("0.99999"), None) != base
    assert fold(Decimal(1), EXPIRES) != base
    assert fold(Decimal(1), None) == base
