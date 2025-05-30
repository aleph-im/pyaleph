import asyncio
import logging
from typing import Dict, Union

from aleph_message.models import Chain
from configmanager import Config

from aleph.types.db_session import AsyncDbSessionFactory

from .abc import ChainReader, ChainWriter
from .bsc import BscConnector
from .chain_data_service import ChainDataService, PendingTxPublisher
from .ethereum import EthereumConnector
from .nuls2 import Nuls2Connector
from .tezos import TezosConnector

LOGGER = logging.getLogger(__name__)


class ChainConnector:
    """
    Service in charge of managing read/write links to blockchains.
    This consists mostly of starting fetcher and publisher tasks for each chain and let the chain-specific
    code do the heavy lifting.
    """

    readers: Dict[Chain, ChainReader]
    writers: Dict[Chain, ChainWriter]

    def __init__(
        self,
        session_factory: AsyncDbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
        chain_data_service: ChainDataService,
    ):
        self._session_factory = session_factory
        self.pending_tx_publisher = pending_tx_publisher
        self._chain_data_service = chain_data_service

        self.readers = {}
        self.writers = {}

        self._register_chains()

    async def chain_reader_task(self, chain: Chain, config: Config):
        connector = self.readers[chain]

        while True:
            try:
                LOGGER.info("Fetching on-chain data for %s...", chain)
                await connector.fetcher(config)
            except Exception:
                LOGGER.exception(
                    "Chain reader task for %s failed, retrying in 60 seconds.", chain
                )

            await asyncio.sleep(60)

    async def chain_writer_task(self, chain: Chain, config: Config):
        connector = self.writers[chain]

        while True:
            try:
                await connector.packer(config)
            except Exception:
                LOGGER.exception(
                    "Chain writer task for %s failed, relaunching in 10 seconds.", chain
                )
                await asyncio.sleep(10)

    async def chain_event_loop(self, config: Config):
        """
        Starts the listener and publisher tasks for all supported chains.
        """

        listener_tasks = []
        publisher_tasks = []

        if config.bsc.enabled.value:
            listener_tasks.append(self.chain_reader_task(Chain.BSC, config))

        if config.ethereum.enabled.value:
            listener_tasks.append(self.chain_reader_task(Chain.ETH, config))
            if config.ethereum.packing_node.value:
                publisher_tasks.append(self.chain_writer_task(Chain.ETH, config))

        if config.nuls2.enabled.value:
            listener_tasks.append(self.chain_reader_task(Chain.NULS2, config))
            if config.nuls2.packing_node.value:
                publisher_tasks.append(self.chain_writer_task(Chain.NULS2, config))

        if config.tezos.enabled.value:
            listener_tasks.append(self.chain_reader_task(Chain.TEZOS, config))

        await asyncio.gather(*(listener_tasks + publisher_tasks))

    def _add_chain(self, chain: Chain, connector: Union[ChainReader, ChainWriter]):
        if isinstance(connector, ChainReader):
            self.readers[chain] = connector
        if isinstance(connector, ChainWriter):
            self.writers[chain] = connector

    def _register_chains(self):
        self._add_chain(
            Chain.BSC,
            BscConnector(
                session_factory=self._session_factory,
                pending_tx_publisher=self.pending_tx_publisher,
            ),
        )
        self._add_chain(
            Chain.NULS2,
            Nuls2Connector(
                session_factory=self._session_factory,
                pending_tx_publisher=self.pending_tx_publisher,
                chain_data_service=self._chain_data_service,
            ),
        )
        self._add_chain(
            Chain.ETH,
            EthereumConnector(
                session_factory=self._session_factory,
                pending_tx_publisher=self.pending_tx_publisher,
                chain_data_service=self._chain_data_service,
            ),
        )
        self._add_chain(
            Chain.TEZOS,
            TezosConnector(
                session_factory=self._session_factory,
                pending_tx_publisher=self.pending_tx_publisher,
            ),
        )
