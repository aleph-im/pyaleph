import datetime as dt

import pytest
import pytz
from aleph_message.models import Chain
from sqlalchemy import select

from aleph.db.accessors.chains import (
    IndexerMultiRange,
    get_indexer_multirange,
    get_last_height,
    get_missing_indexer_datetime_multirange,
    update_indexer_multirange,
    upsert_chain_sync_status,
)
from aleph.db.models.chains import ChainSyncStatusDb, IndexerSyncStatusDb
from aleph.toolkit.range import MultiRange, Range
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import AsyncDbSessionFactory


@pytest.mark.asyncio
async def test_get_last_height(session_factory: AsyncDbSessionFactory):
    sync_type = ChainEventType.SYNC
    eth_sync_status = ChainSyncStatusDb(
        chain=Chain.ETH,
        type=sync_type,
        height=123,
        last_update=pytz.utc.localize(dt.datetime(2022, 10, 1)),
    )

    async with session_factory() as session:
        session.add(eth_sync_status)
        await session.commit()

    async with session_factory() as session:
        height = await get_last_height(
            session=session, chain=Chain.ETH, sync_type=sync_type
        )

    assert height == eth_sync_status.height


@pytest.mark.asyncio
async def test_get_last_height_no_data(session_factory: AsyncDbSessionFactory):
    async with session_factory() as session:
        height = await get_last_height(
            session=session, chain=Chain.NULS2, sync_type=ChainEventType.SYNC
        )

    assert height is None


@pytest.mark.asyncio
async def test_upsert_chain_sync_status_insert(session_factory: AsyncDbSessionFactory):
    chain = Chain.ETH
    sync_type = ChainEventType.SYNC
    update_datetime = pytz.utc.localize(dt.datetime(2022, 11, 1))
    height = 10

    async with session_factory() as session:
        await upsert_chain_sync_status(
            session=session,
            chain=chain,
            sync_type=sync_type,
            height=height,
            update_datetime=update_datetime,
        )
        await session.commit()

    async with session_factory() as session:

        chain_sync_status = (
            await session.execute(
                select(ChainSyncStatusDb).where(ChainSyncStatusDb.chain == chain)
            )
        ).scalar_one()

    assert chain_sync_status.chain == chain
    assert chain_sync_status.type == sync_type
    assert chain_sync_status.height == height
    assert chain_sync_status.last_update == update_datetime


@pytest.mark.asyncio
async def test_upsert_peer_replace(session_factory: AsyncDbSessionFactory):
    existing_entry = ChainSyncStatusDb(
        chain=Chain.TEZOS,
        type=ChainEventType.SYNC,
        height=1000,
        last_update=pytz.utc.localize(dt.datetime(2023, 2, 6)),
    )

    async with session_factory() as session:
        session.add(existing_entry)
        await session.commit()

    new_height = 1001
    new_update_datetime = pytz.utc.localize(dt.datetime(2023, 2, 7))

    async with session_factory() as session:
        await upsert_chain_sync_status(
            session=session,
            chain=existing_entry.chain,
            sync_type=ChainEventType.SYNC,
            height=new_height,
            update_datetime=new_update_datetime,
        )
        await session.commit()

    async with session_factory() as session:
        chain_sync_status = (
            await session.execute(
                select(ChainSyncStatusDb).where(
                    ChainSyncStatusDb.chain == existing_entry.chain
                )
            )
        ).scalar_one()

    assert chain_sync_status.chain == existing_entry.chain
    assert chain_sync_status.type == existing_entry.type
    assert chain_sync_status.height == new_height
    assert chain_sync_status.last_update == new_update_datetime


@pytest.fixture
def indexer_multirange():
    return IndexerMultiRange(
        chain=Chain.ETH,
        event_type=ChainEventType.SYNC,
        datetime_multirange=MultiRange(
            Range(
                dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
                upper_inc=True,
            ),
            Range(
                dt.datetime(2021, 6, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
                upper_inc=True,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_get_indexer_multirange(session_factory: AsyncDbSessionFactory):
    chain = Chain.ETH
    event_type = ChainEventType.SYNC

    ranges = [
        IndexerSyncStatusDb(
            chain=chain,
            event_type=event_type,
            start_block_datetime=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            end_block_datetime=dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
            last_updated=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            start_included=True,
            end_included=False,
        ),
        IndexerSyncStatusDb(
            chain=chain,
            event_type=event_type,
            start_block_datetime=dt.datetime(2021, 6, 1, tzinfo=dt.timezone.utc),
            end_block_datetime=dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
            last_updated=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            start_included=True,
            end_included=True,
        ),
        IndexerSyncStatusDb(
            chain=chain,
            event_type=event_type,
            start_block_datetime=dt.datetime(2022, 6, 1, tzinfo=dt.timezone.utc),
            end_block_datetime=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            last_updated=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            start_included=False,
            end_included=True,
        ),
    ]

    async with session_factory() as session:
        session.add_all(ranges)
        await session.commit()

    async with session_factory() as session:
        db_multirange = await get_indexer_multirange(
            session=session, chain=chain, event_type=event_type
        )

    assert db_multirange.chain == chain
    assert db_multirange.event_type == event_type
    assert db_multirange.datetime_multirange == MultiRange(
        *[rng.to_range() for rng in ranges]
    )


async def test_update_indexer_multirange(
    indexer_multirange: IndexerMultiRange, session_factory: AsyncDbSessionFactory
):
    async with session_factory() as session:
        await update_indexer_multirange(
            session=session, indexer_multirange=indexer_multirange
        )
        await session.commit()

    async with session_factory() as session:
        indexer_multirange_db = await get_indexer_multirange(
            session=session,
            chain=indexer_multirange.chain,
            event_type=indexer_multirange.event_type,
        )

        assert indexer_multirange_db == indexer_multirange


async def test_get_missing_indexer_datetime_multirange(
    indexer_multirange: IndexerMultiRange, session_factory: AsyncDbSessionFactory
):
    async with session_factory() as session:
        await update_indexer_multirange(
            session=session, indexer_multirange=indexer_multirange
        )
        await session.commit()

    async with session_factory() as session:
        new_multirange = MultiRange(
            Range(
                dt.datetime(2019, 1, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                upper_inc=True,
            )
        )
        dt_multirange = await get_missing_indexer_datetime_multirange(
            session=session,
            chain=indexer_multirange.chain,
            event_type=indexer_multirange.event_type,
            indexer_multirange=new_multirange,
        )

        assert dt_multirange == MultiRange(
            Range(
                new_multirange.ranges[0].lower,
                indexer_multirange.datetime_multirange.ranges[0].lower,
                upper_inc=False,
            ),
            Range(
                indexer_multirange.datetime_multirange.ranges[0].upper,
                indexer_multirange.datetime_multirange.ranges[1].lower,
                lower_inc=False,
                upper_inc=False,
            ),
            Range(
                indexer_multirange.datetime_multirange.ranges[1].upper,
                new_multirange.ranges[0].upper,
                lower_inc=False,
                upper_inc=True,
            ),
        )
