"""Tests for the skip-if-clean credit balances repair (screens + orchestration)."""

import datetime as dt

from aleph.db.accessors.balances import (
    get_credit_repair_state,
    upsert_credit_repair_state,
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
