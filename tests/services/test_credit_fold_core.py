import datetime as dt
import hashlib
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

import aleph.services.credits.fold as fold_module
from aleph.db.accessors.credit_ledger import get_fold_state
from aleph.db.models.credit_ledger import (
    CreditCheckpointDb,
    CreditLedgerDb,
    CreditLotDb,
)
from aleph.services.credits.fold import (
    ExclusionInput,
    ExecutionInput,
    GenesisInput,
    PeriodInputs,
    StorageInput,
    TopupInput,
    TransferInput,
    apply_period,
    period_hash,
    refold_from,
    write_checkpoint,
)

A = "0xaaaa"
B = "0xbbbb"
C = "0xcccc"


def P(n: int) -> dt.datetime:
    return dt.datetime(2026, 10, 20, tzinfo=dt.timezone.utc) + dt.timedelta(hours=n)


def topup(address, credits, key):
    return TopupInput(
        address=address, credits=credits, business_key=key, source_ref=key
    )


def execution(owner, vm, amount, node="node1", payment_type="credit", reason=None):
    # rate 1 credit/s keeps amounts exact: amount == seconds
    return ExecutionInput(
        vm=vm,
        node=node,
        owner=owner,
        payment_type=payment_type,
        seconds=amount,
        rate=Decimal(1),
        excluded_reason=reason,
    )


def rows(session, period=None):
    stmt = select(CreditLedgerDb).order_by(CreditLedgerDb.id)
    if period is not None:
        stmt = stmt.where(CreditLedgerDb.period == period)
    return list(session.execute(stmt).scalars())


def lots(session):
    return [
        (lot.ledger_id, lot.address, lot.amount_remaining, lot.expiration)
        for lot in session.execute(
            select(CreditLotDb).order_by(CreditLotDb.ledger_id)
        ).scalars()
    ]


def balance(session, address):
    return sum(amount for _, addr, amount, _ in lots(session) if addr == address)


def test_kind_order_topup_before_execution_in_same_hour(session_factory):
    with session_factory() as session:
        # Execution listed first in the inputs: the kind order still applies the
        # top-up before draining.
        result = apply_period(
            session,
            P(0),
            PeriodInputs(
                executions=[execution(A, "vm1", 60)],
                topups=[topup(A, 100, "1:0xt1:0")],
            ),
        )
        session.commit()

        assert result.rows_written == 2
        by_kind = {row.kind: row for row in rows(session)}
        assert by_kind["topup"].amount == 100
        assert by_kind["execution"].amount == -60
        assert by_kind["execution"].business_key == f"{int(P(0).timestamp())}:vm1:node1"
        assert by_kind["execution"].address == A
        assert balance(session, A) == 40


def test_overdraw_is_recorded_in_full_and_lots_go_to_zero(session_factory):
    with session_factory() as session:
        apply_period(
            session,
            P(0),
            PeriodInputs(
                topups=[topup(A, 10, "1:0xt1:0")],
                executions=[execution(A, "vm1", 25)],
            ),
        )
        session.commit()

        (exec_row,) = [r for r in rows(session) if r.kind == "execution"]
        assert exec_row.amount == -25
        assert exec_row.excluded_reason is None
        assert [amount for _, _, amount, _ in lots(session)] == [0]


def test_transfer_short_is_excluded_whole(session_factory):
    with session_factory() as session:
        apply_period(session, P(0), PeriodInputs(topups=[topup(A, 10, "1:0xt1:0")]))
        apply_period(
            session,
            P(1),
            PeriodInputs(
                transfers=[
                    TransferInput(
                        message_hash="msg1",
                        sender=A,
                        entries=[(B, 8, None), (C, 7, None)],
                    )
                ]
            ),
        )
        session.commit()

        p1_rows = rows(session, P(1))
        assert len(p1_rows) == 1
        (row,) = p1_rows
        assert row.kind == "transfer_out"
        assert row.business_key == "msg1:0"
        assert row.address == A
        assert row.amount == 0
        assert row.excluded_reason == "insufficient_balance"
        assert balance(session, A) == 10
        assert balance(session, B) == 0
        assert balance(session, C) == 0


def test_transfer_caps_expiration_to_source_lot(session_factory):
    genesis_exp = P(0) + dt.timedelta(days=10)
    requested_exp = P(0) + dt.timedelta(days=30)
    with session_factory() as session:
        apply_period(
            session,
            P(0),
            PeriodInputs(
                genesis=[
                    GenesisInput(
                        address=A,
                        credits=5,
                        business_key="snap:0",
                        expiration=genesis_exp,
                    )
                ],
                topups=[topup(A, 10, "1:0xt1:0")],
            ),
        )
        apply_period(
            session,
            P(1),
            PeriodInputs(
                transfers=[
                    TransferInput(
                        message_hash="msg1",
                        sender=A,
                        entries=[(B, 12, requested_exp), (C, 2, None)],
                    )
                ]
            ),
        )
        session.commit()

        p1 = {(r.kind, r.business_key): r for r in rows(session, P(1))}
        assert p1[("transfer_out", "msg1:0")].amount == -14
        assert p1[("transfer_out", "msg1:0")].address == A
        # B: 5 from the genesis lot (capped to its expiration), 7 from the top-up
        # lot (no expiration, so the requested one applies).
        assert p1[("transfer_in", "msg1:1")].address == B
        assert p1[("transfer_in", "msg1:1")].amount == 5
        assert p1[("transfer_in", "msg1:1")].expiration == genesis_exp
        assert p1[("transfer_in", "msg1:2")].address == B
        assert p1[("transfer_in", "msg1:2")].amount == 7
        assert p1[("transfer_in", "msg1:2")].expiration == requested_exp
        # C asked for no expiration and drains the top-up lot: none.
        assert p1[("transfer_in", "msg1:3")].address == C
        assert p1[("transfer_in", "msg1:3")].amount == 2
        assert p1[("transfer_in", "msg1:3")].expiration is None
        assert len(p1) == 4

        assert balance(session, A) == 1
        b_lots = sorted(
            (amount, exp) for _, addr, amount, exp in lots(session) if addr == B
        )
        assert b_lots == [(5, genesis_exp), (7, requested_exp)]
        assert balance(session, C) == 2


def test_expired_lot_is_not_drained(session_factory):
    with session_factory() as session:
        apply_period(
            session,
            P(0),
            PeriodInputs(
                genesis=[
                    GenesisInput(
                        address=A, credits=5, business_key="snap:0", expiration=P(1)
                    )
                ]
            ),
        )
        apply_period(session, P(1), PeriodInputs(executions=[execution(A, "vm1", 3)]))
        session.commit()

        (exec_row,) = rows(session, P(1))
        assert exec_row.amount == -3
        assert [amount for _, _, amount, _ in lots(session)] == [5]


def _mixed_inputs():
    return PeriodInputs(
        genesis=[GenesisInput(address=C, credits=50, business_key="snap:0")],
        topups=[topup(A, 100, "1:0xt1:0"), topup(B, 30, "1:0xt2:0")],
        transfers=[
            TransferInput(message_hash="msg1", sender=A, entries=[(B, 10, None)])
        ],
        executions=[
            execution(A, "vm1", 20),
            execution(B, "vm2", 15, node="node2"),
            execution(C, "vm3", 7, payment_type="hold"),
            execution(A, "vm4", 0, reason="over_cap"),
        ],
        storage=[StorageInput(address=B, amount=4, count=2, size_mib=50, seconds=3600)],
        exclusions=[
            ExclusionInput(
                kind="execution",
                business_key=f"{int(P(0).timestamp())}:vm9:node9",
                address=None,
                resource="vm9",
                node_id="node9",
                reason="unknown_message",
            )
        ],
    )


def test_apply_period_is_idempotent(session_factory):
    with session_factory() as session:
        first = apply_period(session, P(0), _mixed_inputs())
        session.commit()
        rows_before = [
            (r.id, r.kind, r.business_key, r.amount, r.excluded_reason)
            for r in rows(session)
        ]
        lots_before = lots(session)

        second = apply_period(session, P(0), _mixed_inputs())
        session.commit()

        assert first.rows_written == len(rows_before) == 11
        assert second.rows_written == 0
        assert second.period_hash == first.period_hash
        assert [
            (r.id, r.kind, r.business_key, r.amount, r.excluded_reason)
            for r in rows(session)
        ] == rows_before
        assert lots(session) == lots_before
        assert balance(session, A) == 70
        assert balance(session, B) == 21
        assert balance(session, C) == 50


def test_period_hash_is_order_independent_of_insertion(session_factory):
    with session_factory() as session:
        inputs = _mixed_inputs()
        first = apply_period(session, P(0), inputs)
        session.commit()
        assert period_hash(session, P(0)) == first.period_hash

        session.execute(delete(CreditLotDb))
        session.execute(delete(CreditLedgerDb))
        session.commit()

        reversed_inputs = PeriodInputs(
            genesis=list(reversed(inputs.genesis)),
            topups=list(reversed(inputs.topups)),
            transfers=inputs.transfers,
            executions=list(reversed(inputs.executions)),
            storage=list(reversed(inputs.storage)),
            exclusions=list(reversed(inputs.exclusions)),
        )
        second = apply_period(session, P(0), reversed_inputs)
        session.commit()

        assert second.period_hash == first.period_hash
        assert period_hash(session, P(0)) == first.period_hash
        # Not the hash of an empty period.
        assert period_hash(session, P(1)) != first.period_hash


def test_checkpoint_and_refold_restore_lots(session_factory):
    with session_factory() as session:
        apply_period(session, P(0), PeriodInputs(topups=[topup(A, 100, "1:0xt1:0")]))
        apply_period(session, P(1), PeriodInputs(executions=[execution(A, "vm1", 30)]))
        write_checkpoint(session, P(1))
        checkpoint_lots = lots(session)
        apply_period(session, P(2), PeriodInputs(executions=[execution(A, "vm1", 20)]))
        state = get_fold_state(session)
        state.last_closed_period = P(2)
        state.dirty_from_period = P(2)
        session.commit()

        cp_rows = {
            row.address: row
            for row in session.execute(
                select(CreditCheckpointDb).where(CreditCheckpointDb.period == P(1))
            ).scalars()
        }
        assert set(cp_rows) == {"", A}
        assert cp_rows[""].lots == []
        assert [lot["amount_remaining"] for lot in cp_rows[A].lots] == [70]
        assert len(cp_rows[A].ledger_hash) == 64
        assert balance(session, A) == 50

        refold_from(session, P(2))
        session.commit()

        assert rows(session, P(2)) == []
        assert lots(session) == checkpoint_lots
        state = get_fold_state(session)
        assert state.last_closed_period == P(1)
        assert state.dirty_from_period is None

        # Mutated P(2) inputs fold on top of the restored lots.
        apply_period(session, P(2), PeriodInputs(executions=[execution(A, "vm1", 5)]))
        session.commit()
        assert balance(session, A) == 65


def test_refold_without_checkpoint_replays_rows(session_factory):
    with session_factory() as session:
        apply_period(
            session,
            P(0),
            PeriodInputs(
                genesis=[
                    GenesisInput(
                        address=B,
                        credits=50,
                        business_key="snap:0",
                        expiration=P(0) + dt.timedelta(days=5),
                    )
                ],
                topups=[topup(A, 100, "1:0xt1:0")],
            ),
        )
        apply_period(
            session,
            P(1),
            PeriodInputs(
                transfers=[
                    TransferInput(
                        message_hash="msg1", sender=B, entries=[(A, 20, None)]
                    ),
                    TransferInput(
                        message_hash="msg2", sender=A, entries=[(C, 500, None)]
                    ),
                ],
                executions=[execution(A, "vm1", 30), execution(B, "vm2", 5)],
                storage=[
                    StorageInput(
                        address=B, amount=3, count=1, size_mib=25, seconds=3600
                    )
                ],
            ),
        )
        lots_after_p1 = lots(session)
        hash_p1 = period_hash(session, P(1))
        apply_period(session, P(2), PeriodInputs(executions=[execution(A, "vm1", 10)]))
        session.commit()

        refold_from(session, P(2))
        session.commit()

        assert lots(session) == lots_after_p1
        assert period_hash(session, P(1)) == hash_p1
        assert rows(session, P(2)) == []
        assert get_fold_state(session).last_closed_period == P(1)
        assert balance(session, A) == 90
        assert balance(session, B) == 22


def test_lots_untouched_when_existence_check_misses(session_factory, monkeypatch):
    """
    A concurrent folder that inserted the rows first: the existence check
    misses, the insert conflicts, and no lot may change for those rows.
    """
    with session_factory() as session:
        first = apply_period(session, P(0), _mixed_inputs())
        session.commit()
        rows_before = [(r.id, r.kind, r.business_key, r.amount) for r in rows(session)]
        lots_before = lots(session)

        monkeypatch.setattr(fold_module, "ledger_row_exists", lambda *a, **kw: False)
        second = apply_period(session, P(0), _mixed_inputs())
        session.commit()

        assert second.rows_written == 0
        assert second.period_hash == first.period_hash
        assert [
            (r.id, r.kind, r.business_key, r.amount) for r in rows(session)
        ] == rows_before
        assert lots(session) == lots_before


def test_lot_change_without_written_row_raises():
    with pytest.raises(RuntimeError):
        fold_module._require_applied(None)


def _fold_chained_transfers(session, checkpoint_p0=False):
    """
    P(0): funding. P(1): three transfers whose message hashes sort in the
    opposite order to their application: "zzz" funds B, "mmm" spends it, "aaa"
    funds B again afterwards. A replay in (kind, key) order would let "mmm"
    drain the "aaa:1" lot, which did not exist yet when "mmm" was folded.
    """
    apply_period(
        session,
        P(0),
        PeriodInputs(
            genesis=[
                GenesisInput(
                    address=A,
                    credits=100,
                    business_key="snap:0",
                    expiration=P(0) + dt.timedelta(days=3),
                )
            ],
            topups=[
                topup(B, 10, "1:0xtb:0"),
                topup(D, 100, "1:0xtd:0"),
            ],
        ),
    )
    if checkpoint_p0:
        # Checkpoint older than P(2) - 1h: refold restores it then replays P(1).
        write_checkpoint(session, P(0))
    apply_period(
        session,
        P(1),
        PeriodInputs(
            transfers=[
                TransferInput(message_hash="zzz", sender=A, entries=[(B, 20, None)]),
                TransferInput(message_hash="mmm", sender=B, entries=[(C, 25, None)]),
                TransferInput(message_hash="aaa", sender=D, entries=[(B, 40, None)]),
            ]
        ),
    )


D = "0xdddd"


@pytest.mark.parametrize("with_checkpoint", [False, True])
def test_refold_keeps_chained_same_hour_transfers(session_factory, with_checkpoint):
    with session_factory() as session:
        _fold_chained_transfers(session, checkpoint_p0=with_checkpoint)
        lots_after_p1 = lots(session)
        hashes = [period_hash(session, P(0)), period_hash(session, P(1))]
        # B spent its top-up (10) and 15 of "zzz:1"; "aaa:1" is untouched.
        b_lots = {
            r.business_key: amount
            for r in rows(session, P(1))
            for lid, addr, amount, _ in lots_after_p1
            if lid == r.id and addr == B
        }
        assert b_lots == {"zzz:1": 5, "aaa:1": 40}

        apply_period(session, P(2), PeriodInputs(executions=[execution(B, "vm1", 3)]))
        session.commit()

        refold_from(session, P(2))
        session.commit()

        assert lots(session) == lots_after_p1
        assert [period_hash(session, P(0)), period_hash(session, P(1))] == hashes
        assert rows(session, P(2)) == []


def test_refold_restores_old_checkpoint_and_replays_rows_after_it(session_factory):
    with session_factory() as session:
        apply_period(session, P(0), PeriodInputs(topups=[topup(A, 100, "1:0xt1:0")]))
        write_checkpoint(session, P(0))
        apply_period(session, P(1), PeriodInputs(executions=[execution(A, "vm1", 30)]))
        apply_period(session, P(2), PeriodInputs(topups=[topup(A, 7, "1:0xt2:0")]))
        lots_after_p2 = lots(session)
        apply_period(session, P(3), PeriodInputs(executions=[execution(A, "vm1", 50)]))
        session.commit()

        refold_from(session, P(3))
        session.commit()

        assert lots(session) == lots_after_p2
        assert balance(session, A) == 77
        assert get_fold_state(session).last_closed_period is None or (
            get_fold_state(session).last_closed_period <= P(2)
        )


def _sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_checkpoint_ledger_hash_chain(session_factory):
    u = [int(P(n).timestamp()) for n in range(3)]
    with session_factory() as session:
        apply_period(session, P(0), PeriodInputs(topups=[topup(A, 100, "1:0xt1:0")]))
        global0 = write_checkpoint(session, P(0))
        apply_period(session, P(1), PeriodInputs(executions=[execution(A, "vm1", 30)]))
        apply_period(session, P(2), PeriodInputs(executions=[execution(A, "vm1", 20)]))
        global2 = write_checkpoint(session, P(2))
        session.commit()

        # Only A has rows, so A's per-period hash is the period hash.
        h = [period_hash(session, P(n)) for n in range(3)]
        a0 = _sha("|" + f"{u[0]}:{h[0]}")
        a2 = _sha(a0 + "|" + f"{u[1]}:{h[1]}|{u[2]}:{h[2]}")

        checkpoints = {
            (row.period, row.address): row
            for row in session.execute(select(CreditCheckpointDb)).scalars()
        }
        assert checkpoints[(P(0), A)].ledger_hash == a0
        assert checkpoints[(P(2), A)].ledger_hash == a2
        assert checkpoints[(P(0), "")].ledger_hash == global0 == _sha(a0)
        assert checkpoints[(P(2), "")].ledger_hash == global2 == _sha(a2)
        assert [lot["amount_remaining"] for lot in checkpoints[(P(2), A)].lots] == [50]


def test_period_start_must_be_an_hour(session_factory):
    with session_factory() as session:
        with pytest.raises(AssertionError):
            apply_period(session, P(0) + dt.timedelta(minutes=1), PeriodInputs())
        with pytest.raises(AssertionError):
            period_hash(session, P(0) + dt.timedelta(seconds=1))
        with pytest.raises(AssertionError):
            refold_from(session, P(0) + dt.timedelta(minutes=30))
