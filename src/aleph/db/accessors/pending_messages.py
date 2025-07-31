import datetime as dt
from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from aleph_message.models import Chain
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.orm import selectinload, undefer
from sqlalchemy.sql import Update

from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.types.db_session import AsyncDbSession


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


async def get_pending_message(
    session: AsyncDbSession, pending_message_id: int
) -> Optional[PendingMessageDb]:
    stmt = (
        select(PendingMessageDb)
        .where(PendingMessageDb.id == pending_message_id)
        .options(selectinload(PendingMessageDb.tx), undefer("*"))
        .execution_options(populate_existing=True)
    )

    result = await session.execute(stmt)
    pending = result.scalar_one_or_none()

    if pending is not None:
        await session.refresh(pending, attribute_names=None)

    return pending


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


async def get_next_pending_messages_from_different_senders(
    session: AsyncDbSession,
    current_time: dt.datetime,
    fetched: bool = True,
    exclude_item_hashes: Optional[Set[str]] = None,
    exclude_addresses: Optional[Set[str]] = None,
    limit: int = 40,
) -> List[PendingMessageDb]:
    """
    Optimized query using content_address and indexed sorting.
    """

    sql_parts = [
        "SELECT DISTINCT ON (content_address) *",
        "FROM pending_messages",
        "WHERE next_attempt <= :current_time",
        "AND fetched = :fetched",
        "AND content IS NOT NULL",
        "AND content_address IS NOT NULL",
    ]

    params = {
        "current_time": current_time,
        "fetched": fetched,
        "limit": limit,
    }

    if exclude_item_hashes:
        hash_keys = []
        for i, h in enumerate(exclude_item_hashes):
            key = f"exclude_hash_{i}"
            hash_keys.append(f":{key}")
            params[key] = h
        sql_parts.append(f"AND item_hash NOT IN ({', '.join(hash_keys)})")

    if exclude_addresses:
        addr_keys = []
        for i, a in enumerate(exclude_addresses):
            key = f"exclude_addr_{i}"
            addr_keys.append(f":{key}")
            params[key] = a
        sql_parts.append(f"AND content_address NOT IN ({', '.join(addr_keys)})")

    sql_parts.append("ORDER BY content_address, next_attempt")
    sql_parts.append("LIMIT :limit")

    stmt = (
        select(PendingMessageDb)
        .from_statement(text("\n".join(sql_parts)))
        .params(**params)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def get_sender_with_pending_batch(
    session,
    batch_size: int,
    exclude_addresses: Set[str],
    exclude_item_hashes: Set[str],
    current_time: dt.datetime,
    candidate_senders: Optional[Set[str]] = None,
) -> Optional[Tuple[str, List[PendingMessageDb]]]:
    """
    Finds the best sender to process a batch from.
    Priority: sender with most pending messages, then oldest pending message.
    """

    conditions = [
        PendingMessageDb.next_attempt <= current_time,
        PendingMessageDb.fetched.is_(True),
        PendingMessageDb.content.isnot(None),
        PendingMessageDb.content_address.isnot(None),
        ~PendingMessageDb.content_address.in_(exclude_addresses),
        ~PendingMessageDb.item_hash.in_(exclude_item_hashes),
    ]

    if candidate_senders:
        conditions.append(PendingMessageDb.content_address.in_(candidate_senders))

    # Step 1: Find sender with most pending messages, then oldest attempt
    subquery = (
        select(
            PendingMessageDb.content_address,
            func.count().label("msg_count"),
            func.min(PendingMessageDb.next_attempt).label("oldest_attempt"),
        )
        .where(*conditions)
        .group_by(PendingMessageDb.content_address)
        .order_by(
            func.count().desc(),  # Most messages
            func.min(PendingMessageDb.next_attempt).asc(),  # Oldest message
        )
        .limit(1)
        .subquery()
    )

    sender_result = await session.execute(select(subquery.c.content_address))
    row = sender_result.first()
    if not row:
        return None

    sender = row[0]

    # Step 2: Fetch batch of messages from that sender
    messages_query = (
        select(PendingMessageDb)
        .where(
            PendingMessageDb.content_address == sender,
            PendingMessageDb.next_attempt <= current_time,
            PendingMessageDb.fetched.is_(True),
            PendingMessageDb.content.isnot(None),
            ~PendingMessageDb.item_hash.in_(exclude_item_hashes),
        )
        .order_by(PendingMessageDb.next_attempt.asc())
        .limit(batch_size)
    )

    result = await session.execute(messages_query)
    messages = result.scalars().all()

    return sender, messages
