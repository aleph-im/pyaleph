import asyncio
import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import (
    Mapping,
    List,
    Optional,
    Tuple,
    TypeVar,
    Type,
    Union,
    Dict,
    Any,
    Sequence,
    Iterable,
)

import aiohttp
from aleph_message.models import Chain, StoreContent, MessageType
from aleph_message.models.item_hash import ItemType
from pydantic import BaseModel
from pydantic.fields import Field

from aleph.chains.common import incoming_chaindata
from aleph.chains.tx_context import TxContext
from aleph.exceptions import InvalidMessageError
from aleph.model.chains import IndexerSyncStatus
from aleph.model.pending import PendingMessage
from aleph.schemas.chains.indexer_response import (
    EntityType,
    IndexerBlockchain,
    IndexerAccountStateResponse,
    IndexerEventResponse,
    MessageEvent,
    SyncEvent,
)
from aleph.schemas.pending_messages import (
    PendingStoreMessage,
    BasePendingMessage,
    parse_message_content,
    get_message_cls,
)
from aleph.toolkit.range import Range, MultiRange
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.chain_sync import ChainEventType
from aleph.utils import get_sha256

LOGGER = logging.getLogger(__name__)


def make_account_state_query(
    blockchain: IndexerBlockchain, accounts: List[str], type_: EntityType
):
    accounts_str = "[" + ", ".join(f'"{account}"' for account in accounts) + "]"

    return """
{
  state: accountState(
    blockchain: %s
    account: %s
    type: %s
  ) {
    blockchain
    type
    indexer
    account
    accurate
    progress
    pending
    processed
  }
}
    """ % (
        blockchain.value,
        accounts_str,
        type_.value,
    )


def make_events_query(
    event_type: ChainEventType,
    blockchain: IndexerBlockchain,
    datetime_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
    block_range: Optional[Tuple[int, int]] = None,
    limit: int = 1000,
):
    if datetime_range and block_range:
        raise ValueError("Only one range of datetimes or blocks can be specified.")
    if not datetime_range and not block_range:
        raise ValueError("A range of datetimes or blocks must be specified.")

    model: Union[Type[MessageEvent], Type[SyncEvent]]

    if event_type == ChainEventType.MESSAGE:
        model = MessageEvent
        event_type_str = "messageEvents"
    else:
        model = SyncEvent
        event_type_str = "syncEvents"

    fields = "\n".join(model.__fields__.keys())
    params: Dict[str, Any] = {
        "blockchain": blockchain.value,
        "limit": limit,
        "reverse": "false",
    }

    if block_range is not None:
        params["startHeight"] = block_range[0]
        params["endHeight"] = block_range[1]

    if datetime_range is not None:
        # The timestamp must be expressed in milliseconds
        params["startDate"] = datetime_range[0].timestamp() * 1000
        params["endDate"] = datetime_range[1].timestamp() * 1000

    return """
{
  %s(%s) {
    %s
  }
}
""" % (
        event_type_str,
        ", ".join(f"{k}: {v}" for k, v in params.items()),
        fields,
    )


T = TypeVar("T", bound=BaseModel)


class AlephIndexerClient:
    def __init__(self, indexer_url: str):
        self.indexer_url = indexer_url
        self.http_session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "AlephIndexerClient":
        self.http_session = aiohttp.ClientSession(self.indexer_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.http_session:
            await self.http_session.close()

    async def _query(self, query: str, model: Type[T]) -> T:
        if self.http_session is None:
            raise ValueError(
                "HTTP session not opened. Use `async with AlephIndexerClient(...):`."
            )

        response = await self.http_session.post("/", json={"query": query})
        response.raise_for_status()
        response_json = await response.json()
        return model.parse_obj(response_json)

    async def fetch_account_state(
        self,
        blockchain: IndexerBlockchain,
        accounts: List[str],
    ) -> IndexerAccountStateResponse:

        query = make_account_state_query(
            blockchain=blockchain, accounts=accounts, type_=EntityType.LOG
        )

        return await self._query(
            query=query,
            model=IndexerAccountStateResponse,
        )

    async def fetch_events(
        self,
        blockchain: IndexerBlockchain,
        event_type: ChainEventType,
        datetime_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
        block_range: Optional[Tuple[int, int]] = None,
        limit: int = 1000,
    ) -> IndexerEventResponse:
        query = make_events_query(
            event_type=event_type,
            blockchain=blockchain,
            block_range=block_range,
            datetime_range=datetime_range,
            limit=limit,
        )

        return await self._query(query=query, model=IndexerEventResponse)


@dataclass
class IndexerSyncState:
    nb_events: int
    last_block: int
    last_block_datetime: dt.datetime


class MessageEventPayload(BaseModel):
    timestamp: float
    addr: str
    message_type: str = Field(alias="msgtype")
    message_content: str = Field(alias="msgcontent")


def indexer_event_to_aleph_message(
    chain: Chain,
    indexer_event: MessageEvent,
) -> Tuple[BasePendingMessage, TxContext]:

    # Indexer timestamps are expressed in milliseconds
    timestamp = indexer_event.timestamp / 1000

    if (message_type_str := indexer_event.type) == "STORE_IPFS":
        content = StoreContent(
            address=indexer_event.address,
            time=timestamp,
            item_type=ItemType.ipfs,
            item_hash=indexer_event.content,
        )
        item_content = content.json()
        message_type = MessageType.store
        message_cls: Type[BasePendingMessage] = PendingStoreMessage

    else:
        try:
            message_type = MessageType(message_type_str)
        except ValueError as e:
            LOGGER.error("Unsupported message type: %s", message_type_str)
            raise InvalidMessageError(
                f"Unsupported message type: {message_type_str}"
            ) from e

        item_content = indexer_event.content
        try:
            content = parse_message_content(
                message_type=MessageType(message_type),
                content_dict=json.loads(item_content),
            )
        except json.JSONDecodeError as e:
            raise InvalidMessageError(
                f"Message content is not JSON: {item_content}"
            ) from e

        message_cls = get_message_cls(message_type)

    item_hash = get_sha256(item_content)

    pending_message = message_cls(
        item_hash=item_hash,
        sender=indexer_event.address,
        chain=chain,
        signature=None,
        type=message_type,
        item_content=item_content,
        content=content,
        item_type=ItemType.inline,
        time=timestamp,
        channel=None,
    )

    tx_context = TxContext(
        chain_name=chain,
        tx_hash=indexer_event.transaction,
        height=indexer_event.height,
        time=timestamp,
        publisher=indexer_event.address,
    )

    return pending_message, tx_context


async def extract_aleph_messages_from_indexer_response(
    chain: Chain,
    indexer_response: IndexerEventResponse,
) -> List[Tuple[BasePendingMessage, TxContext]]:

    events = indexer_response.data.message_events
    return [indexer_event_to_aleph_message(chain, event) for event in events]


async def insert_pending_messages(
    pending_messages: Sequence[Tuple[BasePendingMessage, TxContext]]
):
    for pending_message, tx_context in pending_messages:
        await PendingMessage.collection.insert_one(
            {
                "message": pending_message.dict(exclude={"content"}),
                "source": dict(
                    chain_name=tx_context.chain_name,
                    tx_hash=tx_context.tx_hash,
                    height=tx_context.height,
                    check_message=False,
                ),
            }
        )


def range_to_json(rng: Range[dt.datetime]) -> Dict[str, Any]:
    return {
        "start_block_datetime": rng.lower,
        "end_block_datetime": rng.upper,
        "start_included": rng.lower_inc,
        "end_included": rng.upper_inc,
    }


def range_from_json(d: Dict[str, Any]) -> Range[dt.datetime]:
    return Range(
        lower=d["start_block_datetime"],
        upper=d["end_block_datetime"],
        lower_inc=d["start_included"],
        upper_inc=d["end_included"],
    )


@dataclass
class IndexerMultiRange:
    chain: Chain
    event_type: ChainEventType
    datetime_multirange: MultiRange[dt.datetime]

    def iter_ranges(self) -> Iterable[Range[dt.datetime]]:
        return self.datetime_multirange.ranges

    def to_json(self, last_updated: dt.datetime):
        return {
            "chain": self.chain,
            "event_type": self.event_type,
            "ranges": [range_to_json(rng) for rng in self.datetime_multirange.ranges],
            "last_updated": last_updated,
        }

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]):
        multirange = MultiRange(*[range_from_json(rng) for rng in json_data["ranges"]])

        return cls(
            chain=Chain(json_data["chain"]),
            event_type=ChainEventType(json_data["event_type"]),
            datetime_multirange=multirange,
        )


async def get_indexer_multirange(
    chain: Chain, event_type: ChainEventType
) -> IndexerMultiRange:
    """
    Returns the already synced indexer ranges for the specified chain and event type.
    :param chain: Chain.
    :param event_type: Event type.
    :return: The list of already synced block ranges, sorted by block timestamp.
    """

    sync_status_db = await IndexerSyncStatus.collection.find_one(
        {"chain": chain.value, "event_type": event_type.value}
    )
    if sync_status_db:
        return IndexerMultiRange.from_json(sync_status_db)

    return IndexerMultiRange(
        chain=chain, event_type=event_type, datetime_multirange=MultiRange()
    )


async def get_missing_indexer_datetime_multirange(
    chain: Chain, event_type: ChainEventType, indexer_multirange
) -> MultiRange[dt.datetime]:
    # TODO: this query is inefficient (too much data retrieved, too many rows, code manipulation.
    #       replace it with the range/multirange operations of PostgreSQL 14+ once the MongoDB
    #       version is out the window.
    db_multiranges = await get_indexer_multirange(chain=chain, event_type=event_type)
    return indexer_multirange - db_multiranges.datetime_multirange


async def update_indexer_multirange(indexer_multirange: IndexerMultiRange):
    chain = indexer_multirange.chain
    event_type = indexer_multirange.event_type

    new_values = indexer_multirange.to_json(last_updated=utc_now())

    await IndexerSyncStatus.collection.update_one(
        {"chain": chain.value, "event_type": event_type.value},
        {"$set": {"ranges": new_values["ranges"]}},
        upsert=True,
    )


async def add_indexer_range(
    chain: Chain, event_type: ChainEventType, datetime_range: Range[dt.datetime]
):
    indexer_multirange = await get_indexer_multirange(
        chain=chain, event_type=event_type
    )

    indexer_multirange.datetime_multirange += datetime_range
    await update_indexer_multirange(indexer_multirange=indexer_multirange)


class AlephIndexerReader:

    BLOCKCHAIN_MAP: Mapping[Chain, IndexerBlockchain] = {
        Chain.BSC: IndexerBlockchain.BSC,
        Chain.ETH: IndexerBlockchain.ETHEREUM,
        Chain.SOL: IndexerBlockchain.SOLANA,
    }

    def __init__(
        self,
        chain: Chain,
    ):
        self.chain = chain

        self.blockchain = self.BLOCKCHAIN_MAP[chain]

    async def fetch_range(
        self,
        indexer_client: AlephIndexerClient,
        chain: Chain,
        event_type: ChainEventType,
        datetime_range: Range[dt.datetime],
    ):
        start_datetime = datetime_range.lower
        end_datetime = datetime_range.upper

        limit = 1000

        while True:
            events_response = await indexer_client.fetch_events(
                blockchain=self.blockchain,
                event_type=event_type,
                datetime_range=(start_datetime, end_datetime),
                limit=limit,
            )

            nb_events_fetched = (
                len(events_response.data.message_events)
                if event_type == ChainEventType.MESSAGE
                else len(events_response.data.sync_events)
            )
            LOGGER.info(
                "%s %s event indexer: fetched %d events.",
                chain,
                event_type,
                nb_events_fetched,
            )

            if nb_events_fetched:
                pending_messages = await extract_aleph_messages_from_indexer_response(
                    chain=chain, indexer_response=events_response
                )
                await insert_pending_messages(pending_messages)

                LOGGER.info("%d new txs", len(pending_messages))

                if nb_events_fetched >= limit:
                    last_event_datetime = timestamp_to_datetime(pending_messages[-1][1].time)
                    upper_inc = False
                else:
                    last_event_datetime = end_datetime
                    upper_inc = True

                synced_range = Range(
                    start_datetime,
                    last_event_datetime,
                    upper_inc=upper_inc,
                )
            else:
                synced_range = Range(start_datetime, end_datetime, upper_inc=True)

            LOGGER.info(
                "%s %s indexer: fetched %s",
                chain.value,
                event_type.value,
                str(synced_range),
            )

            await add_indexer_range(
                chain=chain,
                event_type=event_type,
                datetime_range=synced_range,
            )

            if nb_events_fetched < limit:
                LOGGER.info(
                    "%s %s event indexer: done fetching events.",
                    chain.value,
                    event_type.value,
                )
                break

            start_datetime = synced_range.upper

    async def fetch_new_events(
        self,
        indexer_url: str,
        smart_contract_address: str,
        event_type: ChainEventType,
    ) -> None:
        async with AlephIndexerClient(indexer_url=indexer_url) as indexer_client:
            account_state = await indexer_client.fetch_account_state(
                blockchain=self.blockchain,
                accounts=[smart_contract_address],
            )

            if not account_state.data.state:
                LOGGER.warning(
                    "No account data found for %s. Is the indexer up to date?",
                    smart_contract_address,
                )
                return

            indexer_multirange = MultiRange(
                *[
                    Range(rng[0], rng[1], upper_inc=True)
                    for rng in account_state.data.state[0].processed
                ]
            )

            multirange_to_sync = await get_missing_indexer_datetime_multirange(
                chain=self.chain,
                event_type=event_type,
                indexer_multirange=indexer_multirange,
            )

            for range_to_sync in multirange_to_sync:
                LOGGER.info(
                    "%s %s event indexer: fetching %s",
                    self.chain.value,
                    event_type.value,
                    str(range_to_sync),
                )
                await self.fetch_range(
                    indexer_client=indexer_client,
                    chain=self.chain,
                    event_type=event_type,
                    datetime_range=range_to_sync,
                )

    async def fetcher(
        self, indexer_url: str, smart_contract_address: str, event_type: ChainEventType
    ):
        while True:
            try:
                await self.fetch_new_events(
                    indexer_url=indexer_url,
                    smart_contract_address=smart_contract_address,
                    event_type=event_type,
                )
            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching %s %s indexer sync in 10 seconds",
                    self.chain.value,
                    event_type.value,
                )
            else:
                LOGGER.info(
                    "%s %s indexer: processed all transactions, waiting 10 seconds.",
                    self.chain.value,
                    event_type.value,
                )
            await asyncio.sleep(10)
