import datetime as dt
import json

import pytest
from configmanager import Config

from aleph.chains.indexer_reader import AlephIndexerClient
from aleph.schemas.chains.indexer_response import IndexerBlockchain, EntityType
from aleph.types.chain_sync import ChainEventType


@pytest.fixture
def indexer_client(mock_config: Config):
    return AlephIndexerClient(indexer_url=mock_config.aleph.indexer_url.value)


@pytest.mark.skip("Indexer client tests are deactivated by default.")
@pytest.mark.asyncio
async def test_aleph_indexer_fetch_account(indexer_client: AlephIndexerClient):
    account = "0x166fd4299364b21c7567e163d85d78d2fb2f8ad5"

    async with indexer_client:
        response = await indexer_client.fetch_account_state(
            blockchain=IndexerBlockchain.ETHEREUM,
            accounts=[account],
        )

    assert len(response.data.state) == 1
    account_state = response.data.state[0]

    assert account_state.blockchain == IndexerBlockchain.ETHEREUM
    assert account_state.type == EntityType.LOG
    assert account_state.account == account


@pytest.mark.skip("Indexer client tests are deactivated by default.")
@pytest.mark.asyncio
async def test_aleph_indexer_fetch_events(indexer_client: AlephIndexerClient):
    async with indexer_client:
        response = await indexer_client.fetch_events(
            blockchain=IndexerBlockchain.ETHEREUM,
            event_type=ChainEventType.SYNC,
            datetime_range=(
                dt.datetime(2023, 2, 24, 14, 16, 35, tzinfo=dt.timezone.utc),
                dt.datetime(2023, 2, 24, 17, 49, 10, tzinfo=dt.timezone.utc),
            ),
        )

    assert len(response.data.sync_events) == 1
    assert len(response.data.message_events) == 0

    sync_event = response.data.sync_events[0]

    assert (
        sync_event.id
        == "ethereum_16698727_0x166fd4299364b21c7567e163d85d78d2fb2f8ad5_52"
    )
    assert sync_event.timestamp == 1677248195000
    assert sync_event.address == "0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"
    assert sync_event.height == 16698727

    assert json.loads(sync_event.message) == {
        "protocol": "aleph-offchain",
        "version": 1,
        "content": "QmV9tkuBEoSnmSuh7SakL7J33zCuUgDTckA17qyRpz3oDx",
    }
