from aleph_message.models import Chain
from configmanager import Config

from aleph.chains.abc import ChainReader
from aleph.chains.chain_data_service import PendingTxPublisher
from aleph.chains.indexer_reader import AlephIndexerReader
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSessionFactory


class BscConnector(ChainReader):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
    ):
        self.indexer_reader = AlephIndexerReader(
            chain=Chain.BSC,
            session_factory=session_factory,
            pending_tx_publisher=pending_tx_publisher,
        )

    async def fetcher(self, config: Config):
        await self.indexer_reader.fetcher(
            indexer_url=config.aleph.indexer_url.value,
            smart_contract_address=config.bsc.sync_contract.value,
            event_type=ChainEventType.MESSAGE,
        )
