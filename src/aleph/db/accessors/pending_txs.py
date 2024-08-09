from typing import Iterable, Optional

from aleph_message.models import Chain
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from aleph.db.models import ChainTxDb, PendingTxDb
from aleph.types.db_session import DbSession


def get_pending_tx(session: DbSession, tx_hash: str) -> Optional[PendingTxDb]:
    select_stmt = (
        select(PendingTxDb)
        .where(PendingTxDb.tx_hash == tx_hash)
        .options(selectinload(PendingTxDb.tx))
    )
    return (session.execute(select_stmt)).scalar_one_or_none()


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


def upsert_pending_tx(session: DbSession, tx_hash: str) -> None:
    upsert_stmt = insert(PendingTxDb).values(tx_hash=tx_hash).on_conflict_do_nothing()
    session.execute(upsert_stmt)


def delete_pending_tx(session: DbSession, tx_hash: str) -> None:
    delete_stmt = delete(PendingTxDb).where(PendingTxDb.tx_hash == tx_hash)
    session.execute(delete_stmt)
