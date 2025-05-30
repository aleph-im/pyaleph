import datetime as dt
from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence, Set

from aleph_message.models import Chain
from sqlalchemy import and_, delete, func, not_, select, update
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import Update

from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.types.db_session import AsyncDbSession, DbSession


async def get_next_pending_message(
    session: AsyncDbSession,
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
    return (await session.execute(select_stmt)).scalar_one_or_none()


async def get_next_pending_messages(
    session: AsyncDbSession,
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
    return (await session.execute(select_stmt)).scalars()


async def get_pending_messages(
    session: AsyncDbSession, item_hash: str
) -> Iterable[PendingMessageDb]:
    select_stmt = (
        select(PendingMessageDb)
        .order_by(PendingMessageDb.time)
        .where(PendingMessageDb.item_hash == item_hash)
    )
    return (await session.execute(select_stmt)).scalars()


async def get_next_pending_messages_by_address(
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

    if exclude_item_hashes:  # a non-empty set()
        base_stmt = base_stmt.where(
            PendingMessageDb.item_hash.not_in(exclude_item_hashes)
        )

    if exclude_addresses:
        base_stmt = base_stmt.where(
            PendingMessageDb.content["address"].astext.not_in(list(exclude_addresses))
        )

    first_message = (await session.execute(base_stmt)).scalar_one_or_none()

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

    return (await session.execute(match_stmt)).scalars().all()


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


async def count_pending_messages(
    session: AsyncDbSession, chain: Optional[Chain] = None
) -> int:
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

    return (await session.execute(select_stmt)).scalar_one()


def make_pending_message_fetched_statement(
    pending_message: PendingMessageDb, content: Dict[str, Any]
) -> Update:
    update_stmt = (
        update(PendingMessageDb)
        .where(PendingMessageDb.id == pending_message.id)
        .values(fetched=True, content=content, retries=0)
    )
    return update_stmt


async def set_next_retry(
    session: AsyncDbSession,
    pending_message: PendingMessageDb,
    next_attempt: dt.datetime,
) -> None:
    update_stmt = (
        update(PendingMessageDb)
        .where(PendingMessageDb.id == pending_message.id)
        .values(retries=PendingMessageDb.retries + 1, next_attempt=next_attempt)
    )
    await session.execute(update_stmt)


async def delete_pending_message(
    session: AsyncDbSession, pending_message: PendingMessageDb
) -> None:
    await session.execute(
        delete(PendingMessageDb).where(PendingMessageDb.id == pending_message.id)
    )


async def async_get_next_pending_messages_from_different_senders(
    session: AsyncDbSession,
    current_time: dt.datetime,
    fetched: bool = True,
    exclude_item_hashes: Optional[Set[str]] = None,
    exclude_addresses: Optional[Set[str]] = None,
    limit: int = 40,  # Increased default limit to process more messages in parallel
) -> List[PendingMessageDb]:
    """
    Get pending messages from different senders to process in parallel.

    This optimized function maximizes parallelism by fetching more candidates
    in a single query and processing them efficiently.

    Args:
        session: Database session
        current_time: Current time
        fetched: Whether to only return messages that have been fetched
        exclude_item_hashes: Item hashes to exclude (already being processed)
        exclude_addresses: Sender addresses to exclude (already being processed)
        limit: Maximum number of messages to return

    Returns:
        List of pending messages from different senders
    """
    # Use a more efficient query that fetches more potential candidates at once
    # to reduce database roundtrips
    query = (
        select(PendingMessageDb)
        .where(
            and_(
                PendingMessageDb.next_attempt <= current_time,
                PendingMessageDb.fetched == fetched,
            )
        )
        .order_by(
            PendingMessageDb.next_attempt
        )  # Order by next_attempt to prioritize oldest messages
        .limit(
            limit * 5
        )  # Get a much larger batch to ensure we find enough unique senders
    )

    if exclude_item_hashes:
        query = query.where(not_(PendingMessageDb.item_hash.in_(exclude_item_hashes)))

    # Execute query once and process in memory (more efficient)
    result = await session.execute(query)
    candidates = result.scalars().all()

    # Process all candidates in a single pass
    by_sender = {}
    for message in candidates:
        if (
            not message.content
            or not isinstance(message.content, dict)
            or "address" not in message.content
        ):
            continue

        address = message.content["address"]
        if not address:
            continue

        if exclude_addresses and address in exclude_addresses:
            continue

        # Only keep the first message per sender (oldest by next_attempt)
        if address not in by_sender:
            by_sender[address] = message

        # Break early if we've found enough unique senders
        if len(by_sender) >= limit:
            break

    # Return all found messages from different senders
    return list(by_sender.values())


async def async_get_next_pending_message(
    session: AsyncDbSession,
    current_time: dt.datetime,
    offset: int = 0,
    fetched: Optional[bool] = None,
    exclude_item_hashes: Optional[Sequence[str]] = None,
    exclude_addresses: Optional[Sequence[str]] = None,
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

    if exclude_addresses:
        select_stmt = select_stmt.where(
            PendingMessageDb.content["address"].astext.not_in(list(exclude_addresses))
        )

    select_stmt = select_stmt.limit(1)

    result = await session.execute(select_stmt)
    return result.scalar_one_or_none()
