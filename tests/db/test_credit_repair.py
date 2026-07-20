"""Tests for the skip-if-clean credit balances repair (screens + orchestration)."""

import datetime as dt

from sqlalchemy import func, select, update

from aleph.db.accessors.balances import (
    get_credit_repair_state,
    update_credit_balances_distribution,
    update_credit_balances_expense,
    upsert_credit_repair_state,
)
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.repair import (
    REPAIR_POLICY_VERSION,
    _find_conservation_violations,
    _find_order_inversions,
    _find_structural_violations,
    _repair_credit_balances,
)
from aleph.types.db_session import DbSessionFactory


def test_repair_state_roundtrip(session_factory: DbSessionFactory):
    watermark = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    run_time = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        assert get_credit_repair_state(session) is None

        upsert_credit_repair_state(
            session,
            policy_version=1,
            history_watermark=watermark,
            last_run=run_time,
        )
        session.commit()

    with session_factory() as session:
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == 1
        assert state.history_watermark == watermark
        assert state.last_run == run_time

        # Upsert overwrites the single row in place.
        new_watermark = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
        upsert_credit_repair_state(
            session,
            policy_version=2,
            history_watermark=new_watermark,
            last_run=run_time,
        )
        session.commit()

    with session_factory() as session:
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == 2
        assert state.history_watermark == new_watermark


TS_GRANT = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
TS_SPEND = dt.datetime(2026, 1, 2, 12, 0, 0, tzinfo=dt.timezone.utc)


def _grant(session, address, amount, message_hash, ts, expiration=None):
    """Seed one distribution grant through the real writer (cache + history)."""
    entry = {
        "address": address,
        "amount": amount,
        "price": "0.5",
        "tx_hash": "0xabc",
        "provider": "test",
    }
    if expiration is not None:
        entry["expiration"] = int(expiration.timestamp() * 1000)
    update_credit_balances_distribution(
        session=session,
        credits_list=[entry],
        token="ALEPH",
        chain="ETH",
        message_hash=message_hash,
        message_timestamp=ts,
    )


def _spend(session, address, amount, message_hash, ts):
    """Seed one expense through the real writer (drains cache + appends history)."""
    update_credit_balances_expense(
        session=session,
        credits_list=[{"address": address, "amount": amount, "ref": "r"}],
        message_hash=message_hash,
        message_timestamp=ts,
    )


def test_structural_screen_clean(session_factory: DbSessionFactory):
    """A state produced purely by the eager writers passes, including a lot
    drained to exactly zero (the eager writer leaves the zero row)."""
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 1000, "spend_a", TS_SPEND)  # fully drains
        _grant(session, "0xbbb", 500, "grant_b", TS_GRANT)
        session.commit()

        # Sanity: the fully-drained zero row is still present.
        zero_rows = list(
            session.execute(
                select(AlephCreditBalanceDb).where(
                    AlephCreditBalanceDb.address == "0xaaa",
                    AlephCreditBalanceDb.amount_remaining == 0,
                )
            ).scalars()
        )
        assert len(zero_rows) == 1

        assert _find_structural_violations(session) == set()


def test_structural_screen_flags_orphan_lot(session_factory: DbSessionFactory):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        session.add(
            AlephCreditBalanceDb(
                address="0xdead",
                credit_ref="no_such_history_row",
                credit_index=0,
                amount_remaining=10,
                expiration_date=None,
                message_timestamp=TS_GRANT,
            )
        )
        session.commit()

        assert _find_structural_violations(session) == {"0xdead"}


def test_structural_screen_flags_remaining_above_grant(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        session.commit()

        grant_amount = session.execute(
            select(AlephCreditHistoryDb.amount).where(
                AlephCreditHistoryDb.credit_ref == "grant_a"
            )
        ).scalar_one()
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(amount_remaining=grant_amount + 1)
        )
        session.commit()

        assert _find_structural_violations(session) == {"0xaaa"}


def test_structural_screen_flags_mismatched_expiration(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(expiration_date=dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc))
        )
        session.commit()

        assert _find_structural_violations(session) == {"0xaaa"}


def test_conservation_screen_clean(session_factory: DbSessionFactory):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 300, "spend_a", TS_SPEND)
        session.commit()

        assert _find_conservation_violations(session) == set()


def test_conservation_screen_flags_over_drain(session_factory: DbSessionFactory):
    """Cache total below ledger total is a definite bug in any regime."""
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 300, "spend_a", TS_SPEND)
        # Tamper: over-drain the lot to zero (still >= 0 and <= grant, so the
        # structural screen stays quiet — this is S2's job).
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(amount_remaining=0)
        )
        session.commit()

        assert _find_conservation_violations(session) == {"0xaaa"}


def test_conservation_screen_flags_under_drain_strict_address(
    session_factory: DbSessionFactory,
):
    """No expiring grants, never-negative running sum: equality is required,
    so an un-applied drain (cache too high) is flagged."""
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 300, "spend_a", TS_SPEND)
        grant_amount = session.execute(
            select(AlephCreditHistoryDb.amount).where(
                AlephCreditHistoryDb.credit_ref == "grant_a"
            )
        ).scalar_one()
        # Tamper: pretend the drain never touched the lot (== grant, so the
        # structural screen stays quiet).
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(amount_remaining=grant_amount)
        )
        session.commit()

        assert _find_conservation_violations(session) == {"0xaaa"}


def test_conservation_screen_allows_expiry_bounce(session_factory: DbSessionFactory):
    """An expense arriving after the only credit expired legitimately leaves
    cache_sum > ledger_sum; must NOT be flagged."""
    expiry = dt.datetime(2026, 1, 15, tzinfo=dt.timezone.utc)
    after_expiry = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT, expiration=expiry)
        _spend(session, "0xaaa", 400, "spend_a", after_expiry)  # bounces
        session.commit()

        # Sanity: the drain bounced (lot untouched), ledger went down anyway.
        remaining = session.execute(
            select(AlephCreditBalanceDb.amount_remaining).where(
                AlephCreditBalanceDb.credit_ref == "grant_a"
            )
        ).scalar_one()
        assert remaining > 0

        assert _find_conservation_violations(session) == set()


def test_conservation_screen_allows_overdraft(session_factory: DbSessionFactory):
    """Spending more than granted silently drops the excess (both eager and
    replay); cache_sum > ledger_sum is legitimate here; must NOT be flagged."""
    with session_factory() as session:
        _grant(session, "0xaaa", 100, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 500, "spend_a", TS_SPEND)  # over-draw
        session.commit()

        assert _find_conservation_violations(session) == set()


def _history_row(address, amount, ref, ts, last_update, index=0, expiration=None):
    """Raw history row with a controlled insertion time (last_update)."""
    return AlephCreditHistoryDb(
        address=address,
        amount=amount,
        credit_ref=ref,
        credit_index=index,
        expiration_date=expiration,
        message_timestamp=ts,
        last_update=last_update,
    )


T1 = dt.datetime(2026, 3, 1, 10, 0, 0, tzinfo=dt.timezone.utc)
T2 = dt.datetime(2026, 3, 1, 11, 0, 0, tzinfo=dt.timezone.utc)
T3 = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
MT_EARLY = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
MT_MID = dt.datetime(2026, 2, 10, tzinfo=dt.timezone.utc)
MT_LATE = dt.datetime(2026, 2, 20, tzinfo=dt.timezone.utc)


def test_inversion_screen_flags_late_grant_before_applied_drain(
    session_factory: DbSessionFactory,
):
    """A grant inserted (T3) after a drain (T2) it replay-precedes (MT_MID <
    MT_LATE) is an inversion: the drain was applied without seeing it."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_EARLY, T1))
        session.add(_history_row("0xaaa", -500, "e1", MT_LATE, T2))
        session.add(_history_row("0xaaa", 200, "g2", MT_MID, T3))  # late arrival
        session.commit()

        assert _find_order_inversions(session, watermark=T2) == {"0xaaa"}


def test_inversion_screen_respects_watermark(session_factory: DbSessionFactory):
    """The same inversion is NOT re-flagged once the watermark has passed the
    late row's insertion time (a prior repair already reconciled it)."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_EARLY, T1))
        session.add(_history_row("0xaaa", -500, "e1", MT_LATE, T2))
        session.add(_history_row("0xaaa", 200, "g2", MT_MID, T3))
        session.commit()

        assert _find_order_inversions(session, watermark=T3) == set()


def test_inversion_screen_ignores_grant_grant_inversions(
    session_factory: DbSessionFactory,
):
    """Two grants arriving out of order cannot change the final state (lots
    are independent until a drain is involved); must NOT be flagged."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_MID, T1))
        session.add(_history_row("0xaaa", 200, "g2", MT_EARLY, T2))  # late, grant
        session.commit()

        assert _find_order_inversions(session, watermark=T1) == set()


def test_inversion_screen_flags_backdated_drain(session_factory: DbSessionFactory):
    """A drain inserted late with an early timestamp replay-precedes an
    already-applied drain — flag it (rebuild decides if state differs)."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_EARLY, T1))
        session.add(_history_row("0xaaa", -500, "e1", MT_LATE, T2))
        session.add(_history_row("0xaaa", -100, "e2", MT_MID, T3))  # backdated
        session.commit()

        assert _find_order_inversions(session, watermark=T2) == {"0xaaa"}


def _seed_drained_and_live(session):
    """0xaaa: fully drained lot (zero row left by the eager writer — its
    presence/absence distinguishes a skip from a rebuild). 0xbbb: live lot."""
    _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
    _spend(session, "0xaaa", 1000, "spend_a", TS_SPEND)
    _grant(session, "0xbbb", 500, "grant_b", TS_GRANT)


def _zero_row_count(session, address):
    return session.execute(
        select(func.count())
        .select_from(AlephCreditBalanceDb)
        .where(
            AlephCreditBalanceDb.address == address,
            AlephCreditBalanceDb.amount_remaining == 0,
        )
    ).scalar_one()


def _write_current_state(session):
    """State row as a previous successful repair would have left it."""
    max_last_update = session.execute(
        select(func.max(AlephCreditHistoryDb.last_update))
    ).scalar_one()
    upsert_credit_repair_state(
        session,
        policy_version=REPAIR_POLICY_VERSION,
        history_watermark=max_last_update,
        last_run=max_last_update,
    )


def test_repair_skips_when_clean(session_factory: DbSessionFactory):
    with session_factory() as session:
        _seed_drained_and_live(session)
        _write_current_state(session)
        session.commit()

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        # Skip, not rebuild: the eager writer's zero row survived (a rebuild
        # would have purged it).
        assert _zero_row_count(session, "0xaaa") == 1
        # State row refreshed.
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == REPAIR_POLICY_VERSION


def test_repair_bootstrap_rebuilds_everything(session_factory: DbSessionFactory):
    with session_factory() as session:
        _seed_drained_and_live(session)
        session.commit()  # no state row: bootstrap

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        # Full rebuild purges the zero row and creates the state row.
        assert _zero_row_count(session, "0xaaa") == 0
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == REPAIR_POLICY_VERSION
        max_last_update = session.execute(
            select(func.max(AlephCreditHistoryDb.last_update))
        ).scalar_one()
        assert state.history_watermark == max_last_update


def test_repair_full_rebuild_on_policy_version_change(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _seed_drained_and_live(session)
        max_last_update = session.execute(
            select(func.max(AlephCreditHistoryDb.last_update))
        ).scalar_one()
        upsert_credit_repair_state(
            session,
            policy_version=REPAIR_POLICY_VERSION - 1,  # stale policy
            history_watermark=max_last_update,
            last_run=max_last_update,
        )
        session.commit()

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        assert _zero_row_count(session, "0xaaa") == 0  # rebuilt
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == REPAIR_POLICY_VERSION


def test_repair_rebuilds_only_flagged_addresses(session_factory: DbSessionFactory):
    with session_factory() as session:
        _seed_drained_and_live(session)
        _write_current_state(session)
        # Corrupt only 0xbbb: over-drain its lot (S2 flags it).
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.address == "0xbbb")
            .values(amount_remaining=0)
        )
        session.commit()

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        # 0xbbb was rebuilt back to its full grant...
        bbb_remaining = session.execute(
            select(func.sum(AlephCreditBalanceDb.amount_remaining)).where(
                AlephCreditBalanceDb.address == "0xbbb"
            )
        ).scalar_one()
        bbb_granted = session.execute(
            select(AlephCreditHistoryDb.amount).where(
                AlephCreditHistoryDb.credit_ref == "grant_b"
            )
        ).scalar_one()
        assert bbb_remaining == bbb_granted
        # ...while 0xaaa was left alone (zero row survived).
        assert _zero_row_count(session, "0xaaa") == 1
