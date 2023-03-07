import datetime as dt
from typing import List

import pytest
import pytz
from aleph_message.models import Chain, ItemType, MessageType
from sqlalchemy import select

from aleph.db.accessors.messages import (
    get_message_stats_by_address,
    refresh_address_stats_mat_view,
)
from aleph.db.models import MessageDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory

from aleph.db.accessors.chains import upsert_chain_sync_status, get_last_height
from aleph.db.models.chains import ChainSyncStatusDb


@pytest.fixture
def fixture_messages():
    return [
        MessageDb(
            item_hash="e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5",
            chain=Chain.ETH,
            sender="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            signature="0xabfa661aab1a9f58955940ea213387de4773f8b1f244c2236cd4ac5ba7bf2ba902e17074bc4b289ba200807bb40951f4249668b055dc15af145b8842ecfad0601c",
            item_type=ItemType.storage,
            type=MessageType.forget,
            item_content=None,
            content={
                "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                "time": 1645794065.439,
                "aggregates": [],
                "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
                "reason": "None",
            },
            size=154,
            time=timestamp_to_datetime(1645794065.439),
            channel=Channel("TEST"),
        ),
        MessageDb(
            item_hash="aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4",
            chain=Chain.ETH,
            sender="0x1234",
            signature="0x705ca1365a0b794cbfcf89ce13239376d0aab0674d8e7f39965590a46e5206a664bc4b313f3351f313564e033c9fe44fd258492dfbd6c36b089677d73224da0a1c",
            type=MessageType.aggregate,
            item_content='{"address": "0x51A58800b26AA1451aaA803d1746687cB88E0500", "key": "my-aggregate", "time": 1664999873, "content": {"easy": "as", "a-b": "c"}}',
            content={
                "address": "0x1234",
                "key": "my-aggregate",
                "time": 1664999873,
                "content": {"easy": "as", "a-b": "c"},
            },
            item_type=ItemType.inline,
            size=2000,
            time=pytz.utc.localize(dt.datetime.utcfromtimestamp(1664999872)),
            channel=Channel("CHANEL-N5"),
        ),
    ]


@pytest.mark.asyncio
async def test_get_message_stats_by_address(
    session_factory: DbSessionFactory, fixture_messages: List[MessageDb]
):
    # No data test
    with session_factory() as session:
        stats_no_data = get_message_stats_by_address(session)
        assert stats_no_data == []

        # Refresh the materialized view
        session.add_all(fixture_messages)
        session.commit()

        refresh_address_stats_mat_view(session)
        session.commit()

        stats = get_message_stats_by_address(session)
        assert len(stats) == 2

        stats_by_address = {row.address: row for row in stats}
        assert (
            stats_by_address["0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"].type
            == MessageType.forget
        )
        assert (
            stats_by_address["0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"].nb_messages
            == 1
        )
        assert stats_by_address["0x1234"].type == MessageType.aggregate
        assert stats_by_address["0x1234"].nb_messages == 1

        # Filter by address
        stats = get_message_stats_by_address(session, addresses=("0x1234",))
        assert len(stats) == 1
        row = stats[0]
        assert row.address == "0x1234"
        assert row.type == MessageType.aggregate
        assert row.nb_messages == 1
