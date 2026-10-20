import datetime as dt

from sqlalchemy import select

from aleph.db.models.credit_ledger import CreditFoldStateDb
from aleph.types.db_session import DbSession


def get_fold_state(session: DbSession) -> CreditFoldStateDb:
    return session.execute(
        select(CreditFoldStateDb).where(CreditFoldStateDb.id == 1)
    ).scalar_one()


def mark_dirty(session: DbSession, period_start: dt.datetime) -> bool:
    """
    Flag an already-closed period for re-folding.

    Returns True if the period was closed and has been marked, False if there is
    nothing to re-fold yet.
    """
    state = get_fold_state(session)
    if state.last_closed_period is None or period_start > state.last_closed_period:
        return False
    state.dirty_from_period = (
        period_start
        if state.dirty_from_period is None
        else min(state.dirty_from_period, period_start)
    )
    return True
