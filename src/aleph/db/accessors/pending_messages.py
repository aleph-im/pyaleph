import datetime as dt
from typing import Any, Collection, Dict, Iterable, Optional, Sequence

from aleph_message.models import Chain
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import Update

from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.types.db_session import DbSession


def get_next_pending_message(
    session: DbSession,
    current_time: dt.datetime,
    offset: int = 0,
    fetched: Optional[bool] = None,
    exclude_item_hashes: Optional[Sequence[str]] = None,
) -> Optional[PendingMessageDb]:
    select_stmt = (
        select(PendingMessageDb)
        .order_by(PendingMessageDb.next_attempt.asc())
        .offset(offset)
        .options(selectinload(PendingMessageDb.tx))
        .where(PendingMessageDb.next_attempt <= current_time)
    )

    if fetched is not None:
        select_stmt = select_stmt.where(PendingMessageDb.fetched == fetched)

    if exclude_item_hashes:
        select_stmt = select_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    select_stmt = select_stmt.limit(1)
    return (session.execute(select_stmt)).scalar_one_or_none()


def get_next_pending_messages(
    session: DbSession,
    current_time: dt.datetime,
    limit: int = 10000,
    offset: int = 0,
    fetched: Optional[bool] = None,
    exclude_item_hashes: Optional[Collection[str]] = None,
) -> Iterable[PendingMessageDb]:
    select_stmt = (
        select(PendingMessageDb)
        .order_by(PendingMessageDb.next_attempt.asc())
        .offset(offset)
        .options(selectinload(PendingMessageDb.tx))
        .where(PendingMessageDb.next_attempt <= current_time)
    )

    if fetched is not None:
        select_stmt = select_stmt.where(PendingMessageDb.fetched == fetched)

    if exclude_item_hashes:
        select_stmt = select_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    select_stmt = select_stmt.limit(limit)
    return (session.execute(select_stmt)).scalars()


def get_pending_messages(
    session: DbSession, item_hash: str
) -> Iterable[PendingMessageDb]:
    select_stmt = (
        select(PendingMessageDb)
        .order_by(PendingMessageDb.time)
        .where(PendingMessageDb.item_hash == item_hash)
    )
    return session.execute(select_stmt).scalars()


def get_pending_message(
    session: DbSession, pending_message_id: int
) -> Optional[PendingMessageDb]:
    select_stmt = select(PendingMessageDb).where(
        PendingMessageDb.id == pending_message_id
    )
    return session.execute(select_stmt).scalar_one_or_none()


def count_pending_messages(session: DbSession, chain: Optional[Chain] = None) -> int:
    """
    Counts pending messages.

    :param session: DB session.
    :param chain: If specified, the function will only count pending messages that were
                  confirmed on the specified chain.
    """
    select_stmt = select(func.count(PendingMessageDb.id))
    if chain:
        select_stmt = select_stmt.where(ChainTxDb.chain == chain).join(
            ChainTxDb, PendingMessageDb.tx_hash == ChainTxDb.hash
        )

    return (session.execute(select_stmt)).scalar_one()


def make_pending_message_fetched_statement(
    pending_message: PendingMessageDb, content: Dict[str, Any]
) -> Update:
    update_stmt = (
        update(PendingMessageDb)
        .where(PendingMessageDb.id == pending_message.id)
        .values(fetched=True, content=content, retries=0)
    )
    return update_stmt


def set_next_retry(
    session: DbSession, pending_message: PendingMessageDb, next_attempt: dt.datetime
) -> None:
    update_stmt = (
        update(PendingMessageDb)
        .where(PendingMessageDb.id == pending_message.id)
        .values(retries=PendingMessageDb.retries + 1, next_attempt=next_attempt)
    )
    session.execute(update_stmt)


def delete_pending_message(
    session: DbSession, pending_message: PendingMessageDb
) -> None:
    session.execute(
        delete(PendingMessageDb).where(PendingMessageDb.id == pending_message.id)
    )
