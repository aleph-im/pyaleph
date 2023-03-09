from aleph_message.models import Chain
from configmanager import Config

from aleph.chains.indexer_reader import AlephIndexerReader
from aleph.register_chain import register_incoming_worker
from aleph.types.chain_sync import ChainEventType


async def bsc_incoming_worker(config: Config):
    indexer_reader = AlephIndexerReader(chain=Chain.BSC)
    await indexer_reader.fetcher(
        indexer_url=config.aleph.indexer_url.value,
        smart_contract_address=config.bsc.sync_contract.value,
        event_type=ChainEventType.MESSAGE,
    )


register_incoming_worker(Chain.BSC.value, bsc_incoming_worker)
