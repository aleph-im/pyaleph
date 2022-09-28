from enum import IntEnum
from typing import Optional, Dict, Tuple, List

from aleph_message.models import MessageConfirmation
from bson import ObjectId
from pymongo import UpdateOne

from aleph.chains.chain_service import ChainService
from aleph.chains.common import LOGGER
from aleph.exceptions import (
    InvalidMessageError,
    InvalidContent,
    ContentCurrentlyUnavailable,
    UnknownHashError,
)
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.messages import Message, CappedMessage
from aleph.model.pending import PendingMessage
from aleph.permissions import check_sender_authorization
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.schemas.validated_message import (
    make_confirmation_update_query,
    validate_pending_message,
    ValidatedStoreMessage,
    ValidatedForgetMessage,
    make_message_upsert_query,
)
from aleph.storage import StorageService
from .forget import ForgetMessageHandler
from .storage import StoreMessageHandler
from ..chains.tx_context import TxContext


class IncomingStatus(IntEnum):
    FAILED_PERMANENTLY = -1
    RETRYING_LATER = 0
    MESSAGE_HANDLED = 1


class MessageHandler:
    def __init__(self, chain_service: ChainService, storage_service: StorageService):
        self.chain_service = chain_service
        self.storage_service = storage_service

        self.store_message_handler = StoreMessageHandler(storage_service=storage_service)
        self.forget_message_handler = ForgetMessageHandler(storage_service=storage_service)

    @staticmethod
    async def _mark_message_for_retry(
        message: BasePendingMessage,
        chain_name: Optional[str],
        tx_hash: Optional[str],
        height: Optional[int],
        check_message: bool,
        retrying: bool,
        existing_id,
    ):
        message_dict = message.dict(exclude={"content"})

        if not retrying:
            await PendingMessage.collection.insert_one(
                {
                    "message": message_dict,
                    "source": dict(
                        chain_name=chain_name,
                        tx_hash=tx_hash,
                        height=height,
                        check_message=check_message,  # should we store this?
                    ),
                }
            )
        else:
            LOGGER.debug(f"Incrementing for {existing_id}")
            result = await PendingMessage.collection.update_one(
                filter={"_id": ObjectId(existing_id)}, update={"$inc": {"retries": 1}}
            )
            LOGGER.debug(f"Update result {result}")

    @staticmethod
    async def delayed_incoming(
        message: BasePendingMessage,
        tx_context: Optional[TxContext] = None,
        check_message: bool = True,
    ):
        if message is None:
            return
        await PendingMessage.collection.insert_one(
            {
                "message": message.dict(exclude={"content"}),
                "tx_context": tx_context.dict() if tx_context else None,
                "check_message": check_message,
            }
        )

    async def incoming(
        self,
        pending_message: BasePendingMessage,
        tx_context: Optional[TxContext] = None,
        seen_ids: Optional[Dict[Tuple, int]] = None,
        check_message: bool = False,
        retrying: bool = False,
        existing_id: Optional[ObjectId] = None,
    ) -> Tuple[IncomingStatus, List[DbBulkOperation]]:
        """New incoming message from underlying chain.

        For regular messages it will be marked as confirmed
        if existing in database, created if not.
        """

        item_hash = pending_message.item_hash
        sender = pending_message.sender
        confirmations = []
        chain_name = tx_context.chain if tx_context is not None else None
        ids_key = (item_hash, sender, chain_name)

        if tx_context:
            if seen_ids is not None:
                if ids_key in seen_ids.keys():
                    if tx_context.height > seen_ids[ids_key]:
                        return IncomingStatus.MESSAGE_HANDLED, []

            confirmations.append(
                MessageConfirmation(
                    chain=tx_context.chain,
                    hash=tx_context.hash,
                    height=tx_context.height,
                    time=tx_context.time,
                    publisher=tx_context.publisher,
                )
            )

        filters = {
            "item_hash": item_hash,
            "chain": pending_message.chain,
            "sender": pending_message.sender,
            "type": pending_message.type,
        }
        existing = await Message.collection.find_one(
            filters,
            projection={"confirmed": 1, "confirmations": 1, "time": 1, "signature": 1},
        )

        if check_message:
            if existing is None or (existing["signature"] != pending_message.signature):
                # check/sanitize the message if needed
                try:
                    await self.chain_service.verify_signature(pending_message)
                except InvalidMessageError:
                    return IncomingStatus.FAILED_PERMANENTLY, []

        if retrying:
            LOGGER.debug("(Re)trying %s." % item_hash)
        else:
            LOGGER.info("Incoming %s." % item_hash)

        updates: Dict[str, Dict] = {}

        if existing:
            if seen_ids is not None and tx_context is not None:
                if ids_key in seen_ids.keys():
                    if tx_context.height > seen_ids[ids_key]:
                        return IncomingStatus.MESSAGE_HANDLED, []
                    else:
                        seen_ids[ids_key] = tx_context.height
                else:
                    seen_ids[ids_key] = tx_context.height

            LOGGER.debug("Updating %s." % item_hash)

            if confirmations:
                updates = make_confirmation_update_query(confirmations)

        else:
            try:
                content = await self.storage_service.get_message_content(
                    pending_message
                )

            except InvalidContent:
                LOGGER.warning(
                    "Can't get content of object %r, won't retry." % item_hash
                )
                return IncomingStatus.FAILED_PERMANENTLY, []

            except (ContentCurrentlyUnavailable, Exception) as e:
                if not isinstance(e, ContentCurrentlyUnavailable):
                    LOGGER.exception("Can't get content of object %r" % item_hash)
                await self._mark_message_for_retry(
                    message=pending_message,
                    tx_context=tx_context,
                    check_message=check_message,
                    retrying=retrying,
                    existing_id=existing_id,
                )
                return IncomingStatus.RETRYING_LATER, []

            validated_message = validate_pending_message(
                pending_message=pending_message,
                content=content,
                confirmations=confirmations,
            )

            # warning: those handlers can modify message and content in place
            # and return a status. None has to be retried, -1 is discarded, True is
            # handled and kept.
            # TODO: change this, it's messy.
            try:
                if isinstance(validated_message, ValidatedStoreMessage):
                    handling_result = (
                        await self.store_message_handler.handle_new_storage(
                            validated_message
                        )
                    )
                elif isinstance(validated_message, ValidatedForgetMessage):
                    # Handling it here means that there we ensure that the message
                    # has been forgotten before it is saved on the node.
                    # We may want the opposite instead: ensure that the message has
                    # been saved before it is forgotten.
                    handling_result = (
                        await self.forget_message_handler.handle_forget_message(
                            validated_message
                        )
                    )
                else:
                    handling_result = True
            except UnknownHashError:
                LOGGER.warning(
                    f"Invalid IPFS hash for message {item_hash}, won't retry."
                )
                return IncomingStatus.FAILED_PERMANENTLY, []
            except Exception:
                LOGGER.exception("Error using the message type handler")
                handling_result = None

            if handling_result is None:
                LOGGER.debug("Message type handler has failed, retrying later.")
                await self._mark_message_for_retry(
                    message=pending_message,
                    tx_context=tx_context,
                    check_message=check_message,
                    retrying=retrying,
                    existing_id=existing_id,
                )
                return IncomingStatus.RETRYING_LATER, []

            if not handling_result:
                LOGGER.warning(
                    "Message type handler has failed permanently for "
                    "%r, won't retry." % item_hash
                )
                return IncomingStatus.FAILED_PERMANENTLY, []

            if not await check_sender_authorization(validated_message):
                LOGGER.warning("Invalid sender for %s" % item_hash)
                return IncomingStatus.MESSAGE_HANDLED, []

            if seen_ids is not None and tx_context is not None:
                if ids_key in seen_ids.keys():
                    if tx_context.height > seen_ids[ids_key]:
                        return IncomingStatus.MESSAGE_HANDLED, []
                    else:
                        seen_ids[ids_key] = tx_context.height
                else:
                    seen_ids[ids_key] = tx_context.height

            LOGGER.debug("New message to store for %s." % item_hash)

            updates = make_message_upsert_query(validated_message)

        if updates:
            update_op = UpdateOne(filters, updates, upsert=True)
            bulk_ops = [DbBulkOperation(Message, update_op)]

            # Capped collections do not accept updates that increase the size, so
            # we must ignore confirmations.
            if existing is None:
                bulk_ops.append(DbBulkOperation(CappedMessage, update_op))

            return IncomingStatus.MESSAGE_HANDLED, bulk_ops

        return IncomingStatus.MESSAGE_HANDLED, []

    async def process_one_message(self, message: BasePendingMessage, *args, **kwargs):
        """
        Helper function to process a message on the spot.
        """
        status, ops = await self.incoming(message, *args, **kwargs)
        for op in ops:
            await op.collection.collection.bulk_write([op.operation])
