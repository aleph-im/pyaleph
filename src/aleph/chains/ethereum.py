import asyncio
import importlib.resources
import json
import logging
from typing import Any, AsyncIterator, Dict, List, Literal, Self, Tuple, Union

from aiohttp import ClientResponseError
from aleph_message.models import Chain
from configmanager import Config
from eth_account import Account
from eth_typing import URI, ABIEvent, Address, BlockNumber, ChecksumAddress
from hexbytes import HexBytes
from web3 import AsyncHTTPProvider, AsyncWeb3
from web3._utils.events import get_event_data
from web3.contract import AsyncContract
from web3.exceptions import MismatchedABI, Web3RPCError
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from web3.middleware import LocalFilterMiddleware
from web3.types import ENS, LogReceipt

from aleph.db.accessors.chains import get_last_height, upsert_chain_sync_status
from aleph.db.accessors.messages import get_unconfirmed_messages
from aleph.db.accessors.pending_messages import count_pending_messages
from aleph.db.accessors.pending_txs import count_pending_txs
from aleph.db.models.chains import ChainTxDb
from aleph.exceptions import AlephStorageException
from aleph.schemas.chains.tx_context import TxContext
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSessionFactory

from .abc import ChainWriter
from .chain_data_service import ChainDataService, PendingTxPublisher
from .evm import EVMVerifier
from .indexer_reader import AlephIndexerReader

LOGGER = logging.getLogger("chains.ethereum")
LOGGER.setLevel(logging.INFO)
CHAIN_NAME = "ETH"


class GetLogsException(Exception): ...


class TooManyLogsInRange(GetLogsException):
    start_block: BlockNumber
    end_block: BlockNumber | Literal["latest"]


class PaymentRequired(GetLogsException):
    """Raised when the RPC provider returns HTTP 402 Payment Required."""

    pass


def make_web3_client(rpc_url: URI, timeout: int) -> AsyncWeb3:
    web3 = AsyncWeb3(
        AsyncHTTPProvider(
            rpc_url,
            request_kwargs={"timeout": timeout},
        )
    )
    web3.middleware_onion.add(LocalFilterMiddleware)
    web3.eth.set_gas_price_strategy(rpc_gas_price_strategy)

    return web3


async def _get_contract_abi() -> Any:
    contract_abi_resource = (
        importlib.resources.files("aleph.chains.assets") / "ethereum_sc_abi.json"
    )
    with contract_abi_resource.open("r", encoding="utf-8") as f:
        return json.load(f)


async def get_contract(
    web3_client: AsyncWeb3, contract_address: Union[Address, ChecksumAddress, ENS]
) -> AsyncContract:
    return web3_client.eth.contract(
        address=contract_address, abi=await _get_contract_abi()
    )


class EthereumVerifier(EVMVerifier):
    pass


class EthereumConnector(ChainWriter):
    def __init__(
        self,
        web3_client: AsyncWeb3,
        contract: AsyncContract,
        authorized_emitters: List[Address],
        max_gas_price: int,
        start_height: BlockNumber,
        max_block_range: int,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
        chain_data_service: ChainDataService,
    ):
        self.web3_client = web3_client
        self.contract = contract
        self.authorized_emitters = authorized_emitters
        self.max_gas_price = max_gas_price
        self.start_height = start_height
        self.max_block_range = max_block_range
        self.session_factory = session_factory
        self.pending_tx_publisher = pending_tx_publisher
        self.chain_data_service = chain_data_service

        self.indexer_reader = AlephIndexerReader(
            chain=Chain.ETH,
            session_factory=session_factory,
            pending_tx_publisher=pending_tx_publisher,
        )

    @classmethod
    async def new(
        cls,
        config: Config,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
        chain_data_service: ChainDataService,
    ) -> Self:
        web3_client = make_web3_client(
            rpc_url=config.ethereum.api_url.value,
            chain_id=config.ethereum.chain_id.value,
            timeout=config.ethereum.client_timeout.value,
        )
        contract = await get_contract(web3_client, config.ethereum.sync_contract.value)
        return cls(
            web3_client=web3_client,
            contract=contract,
            authorized_emitters=config.ethereum.authorized_emitters.value,
            max_gas_price=config.ethereum.max_gas_price.value,
            start_height=BlockNumber(config.ethereum.start_height.value),
            max_block_range=config.ethereum.max_block_range.value,
            session_factory=session_factory,
            pending_tx_publisher=pending_tx_publisher,
            chain_data_service=chain_data_service,
        )

    async def get_last_height(self, sync_type: ChainEventType) -> BlockNumber:
        """Returns the last height for which we already have the ethereum data."""
        with self.session_factory() as session:
            last_synced_height = get_last_height(
                session=session, chain=Chain.ETH, sync_type=sync_type
            )

        if last_synced_height is None:
            return self.start_height

        return BlockNumber(last_synced_height)

    async def _get_logs_in_block_range(
        self,
        start_block: BlockNumber,
        end_block: BlockNumber | Literal["latest"] = "latest",
    ) -> List[LogReceipt]:
        """
        Retrieves logs from the Aleph message sync contract and handles RPC-specific exceptions.
        """

        try:
            logs = await self.web3_client.eth.get_logs(
                {
                    "address": self.contract.address,
                    "fromBlock": start_block,
                    "toBlock": end_block,
                }
            )
            return logs
        except ClientResponseError as e:
            if e.status == 402:
                raise PaymentRequired(
                    "RPC credits exhausted (HTTP 402 Payment Required)"
                ) from e
            raise
        except Web3RPCError as e:
            # Handle limit exceptions
            if rpc_response := e.rpc_response:
                if rpc_response["error"]["code"] == -32005:
                    raise TooManyLogsInRange(start_block, end_block) from e

            # Unexpected issue, pass the exception to the caller
            raise

    async def _get_all_logs(self, start_block: BlockNumber) -> List[LogReceipt]:
        return await self._get_logs_in_block_range(start_block, "latest")

    async def _get_all_logs_in_batches(
        self, start_block: BlockNumber, max_block_range: int
    ) -> AsyncIterator[LogReceipt]:
        block_range = max_block_range

        while True:
            last_eth_block = await self.web3_client.eth.block_number
            # Note: the range in get_logs is [start, end].
            end_block = min(last_eth_block, BlockNumber(start_block + block_range - 1))

            LOGGER.info(f"Fetching logs in range {start_block}..{end_block}")
            try:
                for log in await self._get_logs_in_block_range(start_block, end_block):
                    yield log

                start_block = end_block + 1
                # On success, reset the range size.
                block_range = self.max_block_range
            except TooManyLogsInRange:
                block_range //= 2
                LOGGER.info(
                    f"Too many logs in range {start_block}..{end_block}, reducing to {block_range} blocks"
                )

            if end_block == last_eth_block:
                break

    async def _get_logs(self, start_block: BlockNumber) -> AsyncIterator[LogReceipt]:
        # First, try to fetch all available blocks
        try:
            for log in await self._get_all_logs(start_block=start_block):
                yield log
            return
        except TooManyLogsInRange:
            LOGGER.info(
                f"Too many logs in range {start_block}..latest, fetching in batches"
            )

        # If that fails, try fetching in batches until we get all logs.
        async for log in self._get_all_logs_in_batches(
            start_block=start_block, max_block_range=self.max_block_range
        ):
            yield log

    async def _request_transactions(
        self, abi: ABIEvent, start_block: BlockNumber
    ) -> AsyncIterator[Tuple[Dict, TxContext]]:
        """Continuously request data from the Ethereum blockchain.
        TODO: support websocket API.
        """

        logs = self._get_logs(start_block=start_block)

        async for log in logs:
            try:
                event_data = get_event_data(self.web3_client.codec, abi, log)
            except MismatchedABI:
                # Ignore message events, they're handled by the indexer reader.
                continue
            LOGGER.info("Handling TX in block %s" % event_data.blockNumber)
            publisher = event_data.args.addr
            timestamp = event_data.args.timestamp

            if publisher in self.authorized_emitters:
                message = event_data.args.message
                try:
                    jdata = json.loads(message)
                    context = TxContext(
                        chain=Chain(CHAIN_NAME),
                        hash=event_data.transactionHash.hex(),
                        time=timestamp,
                        height=event_data.blockNumber,
                        publisher=publisher,
                    )
                    yield jdata, context

                except json.JSONDecodeError:
                    # if it's not valid json, just ignore it...
                    LOGGER.info(
                        "Incoming logic data is not JSON, ignoring. %r" % message
                    )

                except Exception:
                    LOGGER.exception("Can't decode incoming logic data %r" % message)

            else:
                LOGGER.info(
                    "TX with unauthorized emitter %s in block %s",
                    publisher,
                    event_data.blockNumber,
                )

            # Since we got no critical exception, save last received object
            # block height to do next requests from there.
            last_height = event_data.blockNumber
            if last_height:
                with self.session_factory() as session:
                    upsert_chain_sync_status(
                        session=session,
                        chain=Chain.ETH,
                        sync_type=ChainEventType.SYNC,
                        height=last_height,
                        update_datetime=utc_now(),
                    )
                    session.commit()

    async def fetch_ethereum_sync_events(self):
        last_synced_height = await self.get_last_height(sync_type=ChainEventType.SYNC)

        LOGGER.info("Last block is #%d" % last_synced_height)

        try:
            abi = self.contract.events.SyncEvent._get_event_abi()

            while True:
                last_synced_height = await self.get_last_height(
                    sync_type=ChainEventType.SYNC
                )
                async for jdata, context in self._request_transactions(
                    abi=abi, start_block=BlockNumber(last_synced_height + 1)
                ):
                    tx = ChainTxDb.from_sync_tx_context(
                        tx_context=context, tx_data=jdata
                    )
                    with self.session_factory() as session:
                        await self.pending_tx_publisher.add_and_publish_pending_tx(
                            session=session, tx=tx
                        )
                        session.commit()
        except PaymentRequired:
            LOGGER.warning(
                "RPC provider credits exhausted (HTTP 402). Sync blocked until credits are restored."
            )
        finally:
            await self.web3_client.provider.disconnect()

    async def fetch_sync_events_task(self, poll_interval: int):
        while True:
            try:
                await self.fetch_ethereum_sync_events()
            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching Ethereum message sync in 10 seconds"
                )
            else:
                LOGGER.info(
                    f"Processed all transactions, waiting {poll_interval} seconds."
                )
            await asyncio.sleep(poll_interval)

    async def fetcher(self, config: Config):
        message_event_task = self.indexer_reader.fetcher(
            indexer_url=config.aleph.indexer_url.value,
            # The indexer requires the address to be in the same format as the address it was
            # configured with. This appears to be lowercase for now.
            smart_contract_address=config.ethereum.sync_contract.value.lower(),
            event_type=ChainEventType.MESSAGE,
        )
        sync_event_task = self.fetch_sync_events_task(
            poll_interval=config.ethereum.message_delay.value
        )

        await asyncio.gather(message_event_task, sync_event_task)

    async def _broadcast_content(self, account, gas_price: int, nonce, content):
        tx = await self.contract.functions.doEmit(content).build_transaction(
            {
                "chainId": await self.web3_client.eth.chain_id,
                "gasPrice": gas_price,
                "nonce": nonce,
                "from": account.address,
            }
        )
        signed_tx = account.sign_transaction(tx)
        return await self.web3_client.eth.send_raw_transaction(
            signed_tx.raw_transaction
        )

    async def broadcast_messages(
        self,
        account,
        messages,
        nonce: int,
    ) -> HexBytes:
        gas_price = await self.web3_client.eth.generate_gas_price()
        if gas_price is None:
            gas_price = await self.web3_client.eth.gas_price

        if gas_price > self.max_gas_price:
            raise AlephStorageException(
                f"Gas price too high: {gas_price} > {self.max_gas_price}"
            )

        with self.session_factory() as session:
            sync_event_payload = (
                await self.chain_data_service.prepare_sync_event_payload(
                    session=session, messages=messages
                )
            )
            session.commit()

        return await self._broadcast_content(
            account,
            int(gas_price * 1.1),
            nonce,
            sync_event_payload.json(),
        )

    async def packer(self, config: Config):
        try:
            pri_key = HexBytes(config.ethereum.private_key.value)
            account = Account.from_key(pri_key)
            address = account.address

            LOGGER.info("Ethereum Connector set up with address %s" % address)
            i = 0
            while True:
                with self.session_factory() as session:
                    # Wait for sync operations to complete
                    if (count_pending_txs(session=session, chain=Chain.ETH)) or (
                        count_pending_messages(session=session, chain=Chain.ETH)
                    ) > 1000:
                        await asyncio.sleep(30)
                        continue

                    if i >= 100:
                        await asyncio.sleep(30)  # wait three (!!) blocks
                        i = 0

                    nonce = await self.web3_client.eth.get_transaction_count(
                        account.address
                    )

                    messages = list(
                        get_unconfirmed_messages(
                            session=session, limit=10000, chain=Chain.ETH
                        )
                    )

                if messages:
                    LOGGER.info("Chain sync: %d unconfirmed messages" % len(messages))

                    try:
                        response = await self.broadcast_messages(
                            account=account,
                            messages=messages,
                            nonce=nonce,
                        )
                        LOGGER.info("Broadcast %r on %s" % (response, Chain.ETH.value))
                    except Exception:
                        LOGGER.exception(
                            "Error while broadcasting messages to Ethereum"
                        )

                await asyncio.sleep(config.ethereum.commit_delay.value)
                i += 1
        finally:
            await self.web3_client.provider.disconnect()
