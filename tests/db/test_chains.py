import datetime as dt

import pytest
import pytz
from aleph_message.models import Chain
from sqlalchemy import select

from aleph.types.chain_sync import ChainSyncType
from aleph.types.db_session import DbSessionFactory

from aleph.db.accessors.chains import upsert_chain_sync_status, get_last_height
from aleph.db.models.chains import ChainSyncStatusDb


@pytest.mark.asyncio
async def test_get_last_height(session_factory: DbSessionFactory):
    sync_type = ChainSyncType.SYNC
    eth_sync_status = ChainSyncStatusDb(
        chain=Chain.ETH,
        type=sync_type,
        height=123,
        last_update=pytz.utc.localize(dt.datetime(2022, 10, 1)),
    )

    with session_factory() as session:
        session.add(eth_sync_status)
        session.commit()

    with session_factory() as session:
        height = get_last_height(session=session, chain=Chain.ETH, sync_type=sync_type)

    assert height == eth_sync_status.height


@pytest.mark.asyncio
async def test_get_last_height_no_data(session_factory: DbSessionFactory):
    with session_factory() as session:
        height = get_last_height(
            session=session, chain=Chain.NULS2, sync_type=ChainSyncType.SYNC
        )

    assert height is None


@pytest.mark.asyncio
async def test_upsert_chain_sync_status_insert(session_factory: DbSessionFactory):
    chain = Chain.ETH
    sync_type = ChainSyncType.SYNC
    update_datetime = pytz.utc.localize(dt.datetime(2022, 11, 1))
    height = 10

    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=chain,
            sync_type=sync_type,
            height=height,
            update_datetime=update_datetime,
        )
        session.commit()

    with session_factory() as session:

        chain_sync_status = (
            session.execute(
                select(ChainSyncStatusDb).where(ChainSyncStatusDb.chain == chain)
            )
        ).scalar_one()

    assert chain_sync_status.chain == chain
    assert chain_sync_status.type == sync_type
    assert chain_sync_status.height == height
    assert chain_sync_status.last_update == update_datetime


@pytest.mark.asyncio
async def test_upsert_peer_replace(session_factory: DbSessionFactory):
    existing_entry = ChainSyncStatusDb(
        chain=Chain.TEZOS,
        type=ChainSyncType.SYNC,
        height=1000,
        last_update=pytz.utc.localize(dt.datetime(2023, 2, 6)),
    )

    with session_factory() as session:
        session.add(existing_entry)
        session.commit()

    new_height = 1001
    new_update_datetime = pytz.utc.localize(dt.datetime(2023, 2, 7))

    with session_factory() as session:
        upsert_chain_sync_status(
            session=session,
            chain=existing_entry.chain,
            sync_type=ChainSyncType.SYNC,
            height=new_height,
            update_datetime=new_update_datetime,
        )
        session.commit()

    with session_factory() as session:
        chain_sync_status = (
            session.execute(
                select(ChainSyncStatusDb).where(
                    ChainSyncStatusDb.chain == existing_entry.chain
                )
            )
        ).scalar_one()

    assert chain_sync_status.chain == existing_entry.chain
    assert chain_sync_status.type == existing_entry.type
    assert chain_sync_status.height == new_height
    assert chain_sync_status.last_update == new_update_datetime
