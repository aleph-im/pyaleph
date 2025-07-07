from typing import Iterable, Optional

from aleph_message.models import Chain
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from aleph.db.models import ChainTxDb, PendingTxDb
from aleph.types.db_session import AsyncDbSession


async def get_pending_tx(
    session: AsyncDbSession, tx_hash: str
) -> Optional[PendingTxDb]:
    select_stmt = (
        select(PendingTxDb)
        .where(PendingTxDb.tx_hash == tx_hash)
        .options(selectinload(PendingTxDb.tx))
    )
    return (await session.execute(select_stmt)).scalar_one_or_none()


async def get_pending_txs(
    session: AsyncDbSession, limit: int = 200
) -> Iterable[PendingTxDb]:
    select_stmt = (
        select(PendingTxDb)
        .join(ChainTxDb, PendingTxDb.tx_hash == ChainTxDb.hash)
        .order_by(ChainTxDb.datetime.asc())
        .limit(limit)
        .options(selectinload(PendingTxDb.tx))
    )
    return (await session.execute(select_stmt)).scalars()


async def count_pending_txs(
    session: AsyncDbSession, chain: Optional[Chain] = None
) -> int:
    select_stmt = select(func.count(PendingTxDb.tx_hash))
    if chain:
        select_stmt = select_stmt.join(
            ChainTxDb, PendingTxDb.tx_hash == ChainTxDb.hash
        ).where(ChainTxDb.chain == chain)

    return (await session.execute(select_stmt)).scalar_one()


async def upsert_pending_tx(session: AsyncDbSession, tx_hash: str) -> None:
    upsert_stmt = insert(PendingTxDb).values(tx_hash=tx_hash).on_conflict_do_nothing()
    await session.execute(upsert_stmt)


async def delete_pending_tx(session: AsyncDbSession, tx_hash: str) -> None:
    delete_stmt = delete(PendingTxDb).where(PendingTxDb.tx_hash == tx_hash)
    await session.execute(delete_stmt)
