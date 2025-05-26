import datetime as dt
from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence, Set

from aleph_message.models import Chain
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import Update

from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.types.db_session import AsyncDbSession, DbSession


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


def get_next_pending_messages_by_address(
    session: DbSession,
    current_time: dt.datetime,
    fetched: Optional[bool] = None,
    exclude_item_hashes: Optional[Set[str]] = None,
    exclude_addresses: Optional[Set[str]] = None,
    batch_size: int = 100,
) -> List[PendingMessageDb]:
    # Step 1: Get the earliest pending message
    base_stmt = (
        select(PendingMessageDb)
        .where(PendingMessageDb.next_attempt <= current_time)
        .order_by(PendingMessageDb.next_attempt.asc())
        .options(selectinload(PendingMessageDb.tx))
        .limit(1)
    )

    if fetched is not None:
        base_stmt = base_stmt.where(PendingMessageDb.fetched == fetched)

    if exclude_item_hashes:  # a non-empty set()
        base_stmt = base_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    if exclude_addresses:
        base_stmt = base_stmt.where(
            PendingMessageDb.content["address"].astext.not_in(list(exclude_addresses))
        )

    first_message = session.execute(base_stmt).scalar_one_or_none()

    if (
        not first_message
        or not first_message.content
        or "address" not in first_message.content
    ):
        return []

    address = first_message.content["address"]

    # Step 2: Get a batch of messages with that same address in content
    match_stmt = (
        select(PendingMessageDb)
        .where(
            PendingMessageDb.next_attempt <= current_time,
            PendingMessageDb.content["address"].astext == address,
        )
        .order_by(PendingMessageDb.next_attempt.asc())
        .limit(batch_size)  # Limit to batch_size to avoid fetching too many at once
    )

    if fetched is not None:
        match_stmt = match_stmt.where(PendingMessageDb.fetched == fetched)

    if exclude_item_hashes:
        match_stmt = match_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    return session.execute(match_stmt).scalars().all()


async def async_get_next_pending_messages_by_address(
    session: AsyncDbSession,
    current_time: dt.datetime,
    fetched: Optional[bool] = None,
    exclude_item_hashes: Optional[Set[str]] = None,
    exclude_addresses: Optional[Set[str]] = None,
    batch_size: int = 100,
) -> List[PendingMessageDb]:
    # Step 1: Get the earliest pending message
    base_stmt = (
        select(PendingMessageDb)
        .where(PendingMessageDb.next_attempt <= current_time)
        .order_by(PendingMessageDb.next_attempt.asc())
        .options(selectinload(PendingMessageDb.tx))
        .limit(1)
    )

    if fetched is not None:
        base_stmt = base_stmt.where(PendingMessageDb.fetched == fetched)

    if exclude_item_hashes:
        base_stmt = base_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    if exclude_addresses:
        base_stmt = base_stmt.where(
            PendingMessageDb.content["address"].astext.not_in(list(exclude_addresses))
        )

    result = await session.execute(base_stmt)
    first_message = result.scalar_one_or_none()

    if (
        not first_message
        or not first_message.content
        or "address" not in first_message.content
    ):
        return []

    address = first_message.content["address"]

    # Step 2: Get a batch of messages with that same address in content
    match_stmt = (
        select(PendingMessageDb)
        .where(
            PendingMessageDb.next_attempt <= current_time,
            PendingMessageDb.content["address"].astext == address,
        )
        .order_by(PendingMessageDb.next_attempt.asc())
        .limit(batch_size)
    )

    if fetched is not None:
        match_stmt = match_stmt.where(PendingMessageDb.fetched == fetched)

    if exclude_item_hashes:
        match_stmt = match_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    result = await session.execute(match_stmt)
    return result.scalars().all()


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
