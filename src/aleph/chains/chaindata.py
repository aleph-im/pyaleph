import asyncio
import json
from typing import Dict, Optional, List, Any, Mapping, Set, cast, Type, Union

import aio_pika.abc
from aleph_message.models import StoreContent, ItemType, Chain, MessageType
from pydantic import ValidationError

from aleph.chains.common import LOGGER
from aleph.config import get_config
from aleph.db.accessors.chains import upsert_chain_tx
from aleph.db.accessors.files import upsert_tx_file_pin, upsert_file
from aleph.db.accessors.pending_txs import upsert_pending_tx
from aleph.db.models import ChainTxDb, MessageDb
from aleph.exceptions import (
    InvalidContent,
    AlephStorageException,
    ContentCurrentlyUnavailable,
)
from aleph.schemas.chains.indexer_response import MessageEvent, GenericMessageEvent
from aleph.schemas.chains.tezos_indexer_response import (
    MessageEventPayload as TezosMessageEventPayload,
)
from aleph.schemas.txs import PendingTx
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.files import FileType
from aleph.utils import get_sha256

INCOMING_MESSAGE_AUTHORIZED_FIELDS = [
    "item_hash",
    "item_content",
    "item_type",
    "chain",
    "channel",
    "sender",
    "type",
    "time",
    "signature",
]


class ChainDataService:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        storage_service: StorageService,
        pending_tx_exchange: aio_pika.abc.AbstractExchange,
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service
        self.pending_tx_exchange = pending_tx_exchange

    # TODO: split this function in severa
    async def get_chaindata(
        self, session: DbSession, messages: List[MessageDb], bulk_threshold: int = 2000
    ):
        """Returns content ready to be broadcast on-chain (aka chaindata).

        If message length is over bulk_threshold (default 2000 chars), store list
        in IPFS and store the object hash instead of raw list.
        """

        # TODO: this function is used to guarantee that the chain sync protocol is not broken
        #       while shifting to Postgres.
        #       * exclude the useless fields in the DB query directly and get rid of
        #         INCOMING_MESSAGE_AUTHORIZED_FIELDS
        #       * use a Pydantic model to enforce the output format
        def message_to_dict(_message: MessageDb) -> Mapping[str, Any]:
            message_dict = {
                k: v
                for k, v in _message.to_dict().items()
                if k in INCOMING_MESSAGE_AUTHORIZED_FIELDS
            }
            # Convert the time field to epoch
            message_dict["time"] = message_dict["time"].timestamp()
            return message_dict

        message_dicts = [message_to_dict(message) for message in messages]

        chaindata = {
            "protocol": ChainSyncProtocol.ON_CHAIN_SYNC,
            "version": 1,
            "content": {"messages": message_dicts},
        }
        content = json.dumps(chaindata)
        if len(content) > bulk_threshold:
            ipfs_id = await self.storage_service.add_json(
                session=session, value=chaindata
            )
            return json.dumps(
                {
                    "protocol": ChainSyncProtocol.OFF_CHAIN_SYNC,
                    "version": 1,
                    "content": ipfs_id,
                }
            )
        else:
            return content

    @staticmethod
    def _get_sync_messages(tx_content: Mapping[str, Any]):
        return tx_content["messages"]

    def _get_tx_messages_on_chain_protocol(self, tx: ChainTxDb):
        messages = self._get_sync_messages(tx.content)
        if not isinstance(messages, list):
            error_msg = f"Got bad data in tx {tx.chain}/{tx.hash}"
            raise InvalidContent(error_msg)
        return messages

    async def _get_tx_messages_off_chain_protocol(
        self, tx: ChainTxDb, seen_ids: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        config = get_config()

        file_hash = tx.content
        assert isinstance(file_hash, str)

        if seen_ids is not None:
            if file_hash in seen_ids:
                # is it really what we want here?
                LOGGER.debug("Already seen")
                return []
            else:
                LOGGER.debug("Adding to seen_ids")
                seen_ids.add(file_hash)
        try:
            sync_file_content = await self.storage_service.get_json(
                content_hash=file_hash, timeout=60
            )
        except AlephStorageException:
            # Let the caller handle unavailable/invalid content
            raise
        except Exception as e:
            error_msg = f"Can't get content of offchain object {file_hash}"
            LOGGER.exception("%s", error_msg)
            raise ContentCurrentlyUnavailable(error_msg) from e

        try:
            messages = self._get_sync_messages(sync_file_content.value["content"])
        except AlephStorageException:
            LOGGER.debug("Got no message")
            raise

        LOGGER.info("Got bulk data with %d items" % len(messages))
        if config.ipfs.enabled.value:
            try:
                with self.session_factory() as session:
                    # Some chain data files are duplicated, and can be treated in parallel,
                    # hence the upsert.
                    upsert_file(
                        session=session,
                        file_hash=sync_file_content.hash,
                        file_type=FileType.FILE,
                        size=len(sync_file_content.raw_value),
                    )
                    upsert_tx_file_pin(
                        session=session,
                        file_hash=file_hash,
                        tx_hash=tx.hash,
                        created=utc_now(),
                    )
                    session.commit()

                # Some IPFS fetches can take a while, hence the large timeout.
                await asyncio.wait_for(
                    self.storage_service.pin_hash(file_hash), timeout=120
                )
            except asyncio.TimeoutError:
                LOGGER.warning(f"Can't pin hash {file_hash}")
        return messages

    @staticmethod
    def _get_tx_messages_smart_contract_protocol(tx: ChainTxDb) -> List[Dict[str, Any]]:
        """
        Parses a "smart contract" tx and returns the encapsulated Aleph message.

        This function may still be a bit specific to Tezos as this is the first and
        only supported chain, but it is meant to be generic. Update accordingly.
        Message validation should be left to the message processing pipeline.
        """

        payload_model: Union[Type[TezosMessageEventPayload], Type[MessageEvent]] = (
            TezosMessageEventPayload if tx.chain == Chain.TEZOS else MessageEvent
        )

        try:
            payload = cast(GenericMessageEvent, payload_model.parse_obj(tx.content))
        except ValidationError:
            raise InvalidContent(f"Incompatible tx content for {tx.chain}/{tx.hash}")

        message_type = payload.type

        message_dict = {
            "sender": payload.address,
            "chain": tx.chain.value,
            "signature": None,
            "item_type": ItemType.inline,
            "time": tx.datetime.timestamp(),
        }

        if message_type == "STORE_IPFS":
            message_type = MessageType.store.value
            content = StoreContent(
                address=payload.address,
                time=payload.timestamp_seconds,
                item_type=ItemType.ipfs,
                item_hash=payload.content,
            )
            item_content = content.json(exclude_none=True)
        else:
            item_content = payload.content

        message_dict["item_hash"] = get_sha256(item_content)
        message_dict["type"] = message_type
        message_dict["item_content"] = item_content

        return [message_dict]

    async def get_tx_messages(
        self, tx: ChainTxDb, seen_ids: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        match tx.protocol, tx.protocol_version:
            case ChainSyncProtocol.ON_CHAIN_SYNC, 1:
                return self._get_tx_messages_on_chain_protocol(tx)
            case ChainSyncProtocol.OFF_CHAIN_SYNC, 1:
                return await self._get_tx_messages_off_chain_protocol(
                    tx=tx, seen_ids=seen_ids
                )
            case ChainSyncProtocol.SMART_CONTRACT, 1:
                return self._get_tx_messages_smart_contract_protocol(tx)
            case _:
                error_msg = (
                    f"Unknown protocol/version object in tx {tx.chain}/{tx.hash}: "
                    f"{tx.protocol} v{tx.protocol_version}"
                )
                LOGGER.info("%s", error_msg)
                raise InvalidContent(error_msg)

    async def incoming_chaindata(self, pending_tx: PendingTx):
        """Incoming data from a chain.
        Content can be inline of "offchain" through an ipfs hash.
        For now, we only add it to the database, it will be processed later.
        """

        message = aio_pika.message.Message(body=pending_tx.json().encode("utf-8"))
        await self.pending_tx_exchange.publish(
            message,
            routing_key=f"{pending_tx.chain.value}.{pending_tx.publisher}.{pending_tx.hash}",
        )
