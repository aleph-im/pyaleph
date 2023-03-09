import datetime as dt

import pytest
from aleph_message.models import Chain

from aleph.chains.indexer_reader import (
    IndexerMultiRange,
    get_indexer_multirange,
    range_from_json,
    update_indexer_multirange, get_missing_indexer_datetime_multirange,
)
from aleph.model.chains import IndexerSyncStatus
from aleph.toolkit.range import MultiRange, Range
from aleph.types.chain_sync import ChainEventType


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
async def test_get_indexer_multirange(test_db):
    chain = Chain.ETH
    event_type = ChainEventType.SYNC

    sync_status_json = {
        "chain": chain.value,
        "event_type": event_type.value,
        "ranges": [
            {
                "start_block_datetime": dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                "end_block_datetime": dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
                "start_included": True,
                "end_included": False,
            },
            {
                "start_block_datetime": dt.datetime(2021, 6, 1, tzinfo=dt.timezone.utc),
                "end_block_datetime": dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc),
                "start_included": True,
                "end_included": True,
            },
            {
                "start_block_datetime": dt.datetime(2022, 6, 1, tzinfo=dt.timezone.utc),
                "end_block_datetime": dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
                "start_included": False,
                "end_included": True,
            },
        ],
        "last_updated": dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    }

    await IndexerSyncStatus.collection.insert_one(sync_status_json)

    db_multirange = await get_indexer_multirange(chain=chain, event_type=event_type)

    assert db_multirange.chain == chain
    assert db_multirange.event_type == event_type
    assert db_multirange.datetime_multirange == MultiRange(
        *[range_from_json(rng) for rng in sync_status_json["ranges"]]
    )


@pytest.mark.asyncio
async def test_update_indexer_multirange(
    test_db, indexer_multirange: IndexerMultiRange
):
    await update_indexer_multirange(indexer_multirange=indexer_multirange)

    indexer_multirange_db = await get_indexer_multirange(
        chain=indexer_multirange.chain,
        event_type=indexer_multirange.event_type,
    )

    assert indexer_multirange_db == indexer_multirange


@pytest.mark.asyncio
async def test_get_missing_indexer_datetime_multirange(
    test_db, indexer_multirange: IndexerMultiRange
):
    await update_indexer_multirange(indexer_multirange=indexer_multirange)

    new_multirange = MultiRange(
        Range(
            dt.datetime(2019, 1, 1, tzinfo=dt.timezone.utc),
            dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
            upper_inc=True,
        )
    )
    dt_multirange = await get_missing_indexer_datetime_multirange(
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
