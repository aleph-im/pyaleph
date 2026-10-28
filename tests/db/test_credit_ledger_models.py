import datetime as dt

import pytest
from sqlalchemy.exc import IntegrityError

from aleph.db.accessors.credit_ledger import get_fold_state, mark_dirty
from aleph.db.accessors.cron_jobs import get_cron_job
from aleph.db.models.credit_ledger import CreditFoldStateDb, CreditLedgerDb

P0 = dt.datetime(2026, 10, 20, 0, tzinfo=dt.timezone.utc)
P1 = P0 + dt.timedelta(hours=1)
P2 = P0 + dt.timedelta(hours=2)
P3 = P0 + dt.timedelta(hours=3)


def test_mark_dirty_only_for_closed_periods(session_factory):
    with session_factory() as session:
        assert mark_dirty(session, P1) is False  # nothing closed yet
        get_fold_state(session).last_closed_period = P2
        assert mark_dirty(session, P1) is True
        assert mark_dirty(session, P0) is True
        assert get_fold_state(session).dirty_from_period == P0
        assert mark_dirty(session, P3) is False  # not closed, nothing to re-fold


def test_credit_ledger_tables_exist_with_unique_business_key(session_factory):
    with session_factory() as session:
        session.add(
            CreditLedgerDb(
                period=P0, address="0xa", kind="topup", business_key="1:0xt:0", amount=5
            )
        )
        session.commit()
        session.add(
            CreditLedgerDb(
                period=P0, address="0xa", kind="topup", business_key="1:0xt:0", amount=5
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_fold_state_and_cron_row_are_seeded(session_factory):
    with session_factory() as session:
        assert session.get(CreditFoldStateDb, 1) is not None
        cron = get_cron_job(session, "credit_fold")
        assert cron is not None
        assert cron.interval == 300
