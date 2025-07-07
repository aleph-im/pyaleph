import datetime as dt
from typing import Sequence

import pytest
from aleph_message.models import Chain

from aleph.db.accessors.pending_txs import count_pending_txs, get_pending_txs
from aleph.db.models import ChainTxDb, PendingTxDb
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import AsyncDbSessionFactory


@pytest.fixture
def fixture_txs() -> Sequence[PendingTxDb]:
    return [
        PendingTxDb(
            tx=ChainTxDb(
                hash="1",
                chain=Chain.ETH,
                height=1200,
                datetime=dt.datetime(2022, 1, 1),
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
                protocol_version=1,
                content="1",
            )
        ),
        PendingTxDb(
            tx=ChainTxDb(
                hash="2",
                chain=Chain.SOL,
                height=30000000,
                datetime=dt.datetime(2022, 1, 2),
                publisher="SOLMATE",
                protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
                protocol_version=1,
                content="2",
            )
        ),
        PendingTxDb(
            tx=ChainTxDb(
                hash="3",
                chain=Chain.ETH,
                height=1202,
                datetime=dt.datetime(2022, 1, 3),
                publisher="0xabadbabe",
                protocol=ChainSyncProtocol.OFF_CHAIN_SYNC,
                protocol_version=1,
                content="3",
            )
        ),
    ]


def assert_pending_txs_equal(expected: PendingTxDb, actual: PendingTxDb):
    assert expected.tx_hash == actual.tx_hash


@pytest.mark.asyncio
async def test_get_pending_txs(
    session_factory: AsyncDbSessionFactory, fixture_txs: Sequence[PendingTxDb]
):
    async with session_factory() as session:
        session.add_all(fixture_txs)
        await session.commit()

    async with session_factory() as session:
        pending_txs = list(await get_pending_txs(session=session))

    for expected_tx, actual_tx in zip(pending_txs, fixture_txs):
        assert_pending_txs_equal(expected_tx, actual_tx)

    # Test the limit parameter
    async with session_factory() as session:
        pending_txs = list(await get_pending_txs(session=session, limit=1))

    assert pending_txs
    assert len(pending_txs) == 1
    assert_pending_txs_equal(fixture_txs[0], pending_txs[0])


@pytest.mark.asyncio
async def test_count_pending_txs(
    session_factory: AsyncDbSessionFactory, fixture_txs: Sequence[PendingTxDb]
):
    async with session_factory() as session:
        session.add_all(fixture_txs)
        await session.commit()

    async with session_factory() as session:
        nb_txs = await count_pending_txs(session=session)

    assert nb_txs == len(fixture_txs)
