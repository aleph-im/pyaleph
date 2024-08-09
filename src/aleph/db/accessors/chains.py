import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional

from aleph_message.models import Chain
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from aleph.toolkit.range import MultiRange, Range
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSession

from ..models.chains import ChainSyncStatusDb, ChainTxDb, IndexerSyncStatusDb


def get_last_height(
    session: DbSession, chain: Chain, sync_type: ChainEventType
) -> Optional[int]:
    height = (
        session.execute(
            select(ChainSyncStatusDb.height).where(
                (ChainSyncStatusDb.chain == chain)
                & (ChainSyncStatusDb.type == sync_type)
            )
        )
    ).scalar()
    return height


def upsert_chain_tx(session: DbSession, tx: ChainTxDb) -> None:
    insert_stmt = insert(ChainTxDb).values(
        hash=tx.hash,
        chain=tx.chain,
        height=tx.height,
        datetime=tx.datetime,
        publisher=tx.publisher,
        protocol=tx.protocol,
        protocol_version=tx.protocol_version,
        content=tx.content,
    )
    upsert_stmt = insert_stmt.on_conflict_do_nothing()
    session.execute(upsert_stmt)


def upsert_chain_sync_status(
    session: DbSession,
    chain: Chain,
    sync_type: ChainEventType,
    height: int,
    update_datetime: dt.datetime,
) -> None:
    upsert_stmt = (
        insert(ChainSyncStatusDb)
        .values(chain=chain, type=sync_type, height=height, last_update=update_datetime)
        .on_conflict_do_update(
            constraint="chains_sync_status_pkey",
            set_={"height": height, "last_update": update_datetime},
        )
    )
    session.execute(upsert_stmt)


@dataclass
class IndexerMultiRange:
    chain: Chain
    event_type: ChainEventType
    datetime_multirange: MultiRange[dt.datetime]

    def iter_ranges(self) -> Iterable[Range[dt.datetime]]:
        return self.datetime_multirange.ranges


def get_indexer_multirange(
    session: DbSession, chain: Chain, event_type: ChainEventType
) -> IndexerMultiRange:
    """
    Returns the already synced indexer ranges for the specified chain and event type.

    :param session: DB session.
    :param chain: Chain.
    :param event_type: Event type.
    :return: The list of already synced block ranges, sorted by block timestamp.
    """

    select_stmt = (
        select(IndexerSyncStatusDb)
        .where(
            (IndexerSyncStatusDb.chain == chain)
            & (IndexerSyncStatusDb.event_type == event_type)
        )
        .order_by(IndexerSyncStatusDb.start_block_datetime)
    )

    rows = session.execute(select_stmt).scalars()

    datetime_multirange: MultiRange[dt.datetime] = MultiRange()

    for row in rows:
        datetime_multirange += row.to_range()

    return IndexerMultiRange(
        chain=chain,
        event_type=event_type,
        datetime_multirange=datetime_multirange,
    )


def get_missing_indexer_datetime_multirange(
    session: DbSession, chain: Chain, event_type: ChainEventType, indexer_multirange
) -> MultiRange[dt.datetime]:
    # TODO: this query is inefficient (too much data retrieved, too many rows, code manipulation.
    #       replace it with the range/multirange operations of PostgreSQL 14+ once the MongoDB
    #       version is out the window.
    db_multiranges = get_indexer_multirange(
        session=session, chain=chain, event_type=event_type
    )
    return indexer_multirange - db_multiranges.datetime_multirange


def update_indexer_multirange(
    session: DbSession, indexer_multirange: IndexerMultiRange
):
    chain = indexer_multirange.chain
    event_type = indexer_multirange.event_type

    # For now, just delete all matching entries and rewrite them.
    session.execute(
        delete(IndexerSyncStatusDb).where(
            (IndexerSyncStatusDb.chain == chain)
            & (IndexerSyncStatusDb.event_type == event_type)
        )
    )
    update_time = utc_now()
    for datetime_range in indexer_multirange.iter_ranges():
        session.execute(
            insert(IndexerSyncStatusDb).values(
                chain=chain,
                event_type=event_type,
                start_block_datetime=datetime_range.lower,
                start_included=datetime_range.lower_inc,
                end_block_datetime=datetime_range.upper,
                end_included=datetime_range.upper_inc,
                last_updated=update_time,
            )
        )


def add_indexer_range(
    session: DbSession,
    chain: Chain,
    event_type: ChainEventType,
    datetime_range: Range[dt.datetime],
):
    indexer_multirange = get_indexer_multirange(
        session=session, chain=chain, event_type=event_type
    )

    indexer_multirange.datetime_multirange += datetime_range
    update_indexer_multirange(session=session, indexer_multirange=indexer_multirange)
