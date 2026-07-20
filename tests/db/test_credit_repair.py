"""Tests for the skip-if-clean credit balances repair (screens + orchestration)."""

import datetime as dt

from sqlalchemy import select, update

from aleph.db.accessors.balances import (
    get_credit_repair_state,
    update_credit_balances_distribution,
    update_credit_balances_expense,
    upsert_credit_repair_state,
)
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.repair import _find_structural_violations
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
