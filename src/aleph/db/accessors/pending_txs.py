from typing import Optional, Iterable

from aleph_message.models import Chain
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from aleph.db.models import PendingTxDb, ChainTxDb
from aleph.types.db_session import DbSession


def get_pending_txs(session: DbSession, limit: int = 200) -> Iterable[PendingTxDb]:
    select_stmt = (
        select(PendingTxDb)
        .join(ChainTxDb, PendingTxDb.tx_hash == ChainTxDb.hash)
        .order_by(ChainTxDb.datetime.asc())
        .limit(limit)
        .options(selectinload(PendingTxDb.tx))
    )
    return (session.execute(select_stmt)).scalars()


def count_pending_txs(session: DbSession, chain: Optional[Chain] = None) -> int:
    select_stmt = select(func.count(PendingTxDb.tx_hash))
    if chain:
        select_stmt = select_stmt.join(
            ChainTxDb, PendingTxDb.tx_hash == ChainTxDb.hash
        ).where(ChainTxDb.chain == chain)

    return (session.execute(select_stmt)).scalar_one()
