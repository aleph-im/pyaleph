import asyncio
import json
import logging
from typing import List, Sequence, Tuple

import aiohttp
from aleph_message.models import Chain, MessageType, StoreContent, ItemType
from aleph_pytezos.crypto.key import Key
from configmanager import Config

from aleph.chains.common import get_verification_buffer
from aleph.chains.tx_context import TxContext
from aleph.model.chains import Chain as ChainDb
from aleph.model.pending import PendingMessage
from aleph.register_chain import register_verifier, register_incoming_worker
from aleph.schemas.chains.tezos_indexer_response import (
    IndexerResponse,
    SyncStatus,
    IndexerMessageEvent,
)
from aleph.schemas.pending_messages import BasePendingMessage, PendingStoreMessage
from aleph.utils import get_sha256

LOGGER = logging.getLogger(__name__)
CHAIN_NAME = "TEZOS"


async def verify_signature(message: BasePendingMessage) -> bool:
    """
    Verifies the cryptographic signature of a message signed with a Tezos key.
    """

    if message.signature is None:
        LOGGER.warning("'%s': missing signature.", message.item_hash)
        return False

    verification_buffer = get_verification_buffer(message)
    try:
        signature_dict = json.loads(message.signature)
    except json.JSONDecodeError:
        LOGGER.warning("Signature field for Tezos message is not JSON deserializable.")
        return False

    try:
        signature = signature_dict["signature"]
        public_key = signature_dict["publicKey"]
    except KeyError as e:
        LOGGER.warning("'%s' key missing from Tezos signature dictionary.", e.args[0])
        return False

    key = Key.from_encoded_key(public_key)
    # Check that the sender ID is equal to the public key hash
    public_key_hash = key.public_key_hash()

    if message.sender != public_key_hash:
        LOGGER.warning(
            "Sender ID (%s) does not match public key hash (%s)",
            message.sender,
            public_key_hash,
        )

    # Check the signature
    try:
        key.verify(signature, verification_buffer)
    except ValueError:
        LOGGER.warning("Received message with bad signature from %s" % message.sender)
        return False

    return True


register_verifier(CHAIN_NAME, verify_signature)


def make_graphql_status_query():
    return "{indexStatus {status}}"


def make_graphql_query(
    sync_contract_address: str, event_type: str, limit: int, skip: int
):
    return """
{
  indexStatus {
    oldestBlock
    recentBlock
    status
  }
  stats {
    totalEvents
  }
  events(limit: %d, skip: %d, source: "%s", type: "%s") {
    source
    timestamp
    blockHash
    blockLevel
    type
    payload
  }
}
""" % (
        limit,
        skip,
        sync_contract_address,
        event_type,
    )


async def get_indexer_status(http_session: aiohttp.ClientSession) -> SyncStatus:
    response = await http_session.post("/", json={"query": make_graphql_status_query()})
    response.raise_for_status()
    response_json = await response.json()

    return SyncStatus(response_json["data"]["indexStatus"]["status"])


async def fetch_messages(
    http_session: aiohttp.ClientSession,
    sync_contract_address: str,
    event_type: str,
    limit: int,
    skip: int,
) -> IndexerResponse[IndexerMessageEvent]:

    query = make_graphql_query(
        limit=limit,
        skip=skip,
        sync_contract_address=sync_contract_address,
        event_type=event_type,
    )

    response = await http_session.post("/", json={"query": query})
    response.raise_for_status()
    response_json = await response.json()

    return IndexerResponse[IndexerMessageEvent].parse_obj(response_json)


def indexer_event_to_aleph_message(
    indexer_event: IndexerMessageEvent,
) -> Tuple[BasePendingMessage, TxContext]:

    if message_type := indexer_event.payload.message_type != "STORE_IPFS":
        raise ValueError(f"Unexpected message type: {message_type}")

    content = StoreContent(
        address=indexer_event.payload.addr,
        time=indexer_event.payload.timestamp,
        item_type=ItemType.ipfs,
        item_hash=indexer_event.payload.message_content,
    )
    item_content = content.json()
    item_hash = get_sha256(item_content)

    pending_message = PendingStoreMessage(
        item_hash=item_hash,
        sender=indexer_event.payload.addr,
        chain=Chain.TEZOS,
        signature=None,
        type=MessageType.store,
        item_content=StoreContent(
            address=indexer_event.payload.addr,
            time=indexer_event.payload.timestamp,
            item_type=ItemType.ipfs,
            item_hash=indexer_event.payload.message_content,
        ).json(),
        content=content,
        item_type=ItemType.inline,
        time=indexer_event.timestamp.timestamp(),
        channel=None,
    )

    tx_context = TxContext(
        chain_name=Chain.TEZOS,
        tx_hash=indexer_event.block_hash,
        height=indexer_event.block_level,
        time=indexer_event.timestamp.timestamp(),
        publisher=indexer_event.source,
    )

    return pending_message, tx_context


async def extract_aleph_messages_from_indexer_response(
    indexer_response: IndexerResponse[IndexerMessageEvent],
) -> List[Tuple[BasePendingMessage, TxContext]]:

    events = indexer_response.data.events
    return [indexer_event_to_aleph_message(event) for event in events]


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


async def fetch_incoming_messages(config: Config):

    indexer_url = config.tezos.indexer_url.value

    async with aiohttp.ClientSession(indexer_url) as http_session:
        status = await get_indexer_status(http_session)

        if status != SyncStatus.SYNCED:
            LOGGER.warning("Tezos indexer is not yet synced, waiting until it is")
            return

        last_committed_height = (await ChainDb.get_last_height(Chain.TEZOS)) or 0
        limit = 100

        try:
            while True:
                indexer_response_data = await fetch_messages(
                    http_session,
                    sync_contract_address=config.tezos.sync_contract.value,
                    event_type="MessageEvent",
                    limit=limit,
                    skip=last_committed_height,
                )
                pending_messages = await extract_aleph_messages_from_indexer_response(
                    indexer_response_data
                )
                await insert_pending_messages(pending_messages)

                last_committed_height += limit
                if (
                    last_committed_height
                    >= indexer_response_data.data.stats.total_events
                ):
                    last_committed_height = (
                        indexer_response_data.data.stats.total_events
                    )
                    break

        finally:
            await ChainDb.set_last_height(
                chain=Chain.TEZOS, height=last_committed_height
            )


async def tezos_sync_worker(config):
    if config.tezos.enabled.value:
        while True:
            try:
                await fetch_incoming_messages(config)

            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching Tezos message sync in 10 seconds"
                )
            await asyncio.sleep(10)


register_incoming_worker(CHAIN_NAME, tezos_sync_worker)
