import datetime as dt
import logging
from typing import Optional, Dict, Any, Mapping

import psycopg2
import sqlalchemy.exc
from aleph_message.models import MessageType, ItemType
from configmanager import Config
from pydantic import ValidationError
from sqlalchemy import insert

from aleph.chains.chain_service import ChainService
from aleph.db.accessors.files import insert_content_file_pin, upsert_file
from aleph.db.accessors.messages import (
    get_message_by_item_hash,
    make_confirmation_upsert_query,
    make_message_upsert_query,
    make_message_status_upsert_query,
    reject_new_pending_message,
)
from aleph.db.accessors.pending_messages import delete_pending_message
from aleph.db.models import (
    PendingMessageDb,
    MessageDb,
    MessageStatusDb,
)
from aleph.exceptions import (
    InvalidContent,
    ContentCurrentlyUnavailable,
    UnknownHashError,
)
from aleph.handlers.content.aggregate import AggregateMessageHandler
from aleph.handlers.content.content_handler import ContentHandler
from aleph.handlers.content.forget import ForgetMessageHandler
from aleph.handlers.content.post import PostMessageHandler
from aleph.handlers.content.program import ProgramMessageHandler
from aleph.handlers.content.store import StoreMessageHandler
from aleph.schemas.pending_messages import parse_message
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.types.files import FileType
from aleph.types.message_status import (
    InvalidMessageException,
    InvalidSignature,
    MessageContentUnavailable,
    MessageStatus,
    InvalidMessageFormat,
)

LOGGER = logging.getLogger(__name__)


class MessageHandler:
    content_handlers: Dict[MessageType, ContentHandler]

    def __init__(
        self,
        session_factory: DbSessionFactory,
        chain_service: ChainService,
        storage_service: StorageService,
        config: Config,
    ):
        self.session_factory = session_factory
        self.chain_service = chain_service
        self.storage_service = storage_service

        self.content_handlers = {
            MessageType.aggregate: AggregateMessageHandler(),
            MessageType.post: PostMessageHandler(
                balances_addresses=config.aleph.balances.addresses.value,
                balances_post_type=config.aleph.balances.post_type.value,
            ),
            MessageType.program: ProgramMessageHandler(),
            MessageType.store: StoreMessageHandler(storage_service=storage_service),
        }

        self.content_handlers[MessageType.forget] = ForgetMessageHandler(
            content_handlers=self.content_handlers,
        )

    # TODO typing: make this function generic on message type
    def get_content_handler(self, message_type: MessageType) -> ContentHandler:
        return self.content_handlers[message_type]

    async def verify_signature(self, pending_message: PendingMessageDb):
        if pending_message.check_message:
            # TODO: remove type: ignore by deciding the pending message type
            await self.chain_service.verify_signature(pending_message)  # type: ignore

    async def fetch_pending_message(
        self, pending_message: PendingMessageDb
    ) -> MessageDb:
        item_hash = pending_message.item_hash

        try:
            content = await self.storage_service.get_message_content(pending_message)
        except InvalidContent as e:
            error_msg = f"Invalid message content for {item_hash}: {str(e)}"
            LOGGER.warning(error_msg)
            raise InvalidMessageFormat(error_msg)

        except (ContentCurrentlyUnavailable, Exception) as e:
            if not isinstance(e, ContentCurrentlyUnavailable):
                LOGGER.exception("Can't get content of object %s" % item_hash)
            raise MessageContentUnavailable(f"Could not fetch content for {item_hash}")

        try:
            validated_message = MessageDb.from_pending_message(
                pending_message=pending_message,
                content_dict=content.value,
                content_size=len(content.raw_value),
            )
        except ValidationError as e:
            raise InvalidMessageFormat(errors=e.errors()) from e

        return validated_message

    async def fetch_related_content(self, session: DbSession, message: MessageDb):
        content_handler = self.get_content_handler(message.type)

        try:
            await content_handler.fetch_related_content(
                session=session, message=message
            )
        except UnknownHashError as e:
            raise InvalidMessageFormat(
                f"Invalid IPFS hash for message {message.item_hash}"
            ) from e

    @staticmethod
    async def confirm_existing_message(
        session: DbSession,
        existing_message: MessageDb,
        pending_message: PendingMessageDb,
    ):
        if pending_message.signature != existing_message.signature:
            raise InvalidSignature(f"Invalid signature for {pending_message.item_hash}")

        delete_pending_message(session=session, pending_message=pending_message)
        if tx_hash := pending_message.tx_hash:
            session.execute(
                make_confirmation_upsert_query(
                    item_hash=pending_message.item_hash, tx_hash=tx_hash
                )
            )

    async def load_fetched_content(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> PendingMessageDb:
        if pending_message.item_type != ItemType.inline:
            pending_message.fetched = False
            return pending_message

        # We reuse fetch_pending_messages to load the inline content. The check
        # above ensures we will not load content from the network here.
        message = await self.fetch_pending_message(pending_message=pending_message)
        content_handler = self.get_content_handler(message.type)
        is_fetched = await content_handler.is_related_content_fetched(
            session=session, message=message
        )

        pending_message.fetched = is_fetched
        pending_message.content = message.content
        return pending_message

    async def add_pending_message(
        self,
        message_dict: Mapping[str, Any],
        reception_time: dt.datetime,
        tx_hash: Optional[str] = None,
        check_message: bool = True,
    ) -> Optional[PendingMessageDb]:

        # TODO: this implementation is just messy, improve it.
        with self.session_factory() as session:
            try:
                # we don't check signatures yet.
                message = parse_message(message_dict)
            except InvalidMessageException as e:
                LOGGER.warning(e)
                reject_new_pending_message(
                    session=session, pending_message=message_dict, exception=e
                )
                session.commit()
                return None

            pending_message = PendingMessageDb.from_obj(
                message,
                reception_time=reception_time,
                tx_hash=tx_hash,
                check_message=check_message,
            )

            try:
                pending_message = await self.load_fetched_content(
                    session, pending_message
                )
            except InvalidMessageException as e:
                LOGGER.warning("Invalid message: %s - %s", message.item_hash, str(e))
                reject_new_pending_message(
                    session=session, pending_message=message_dict, exception=e
                )
                session.commit()
                return None

            upsert_message_status_stmt = make_message_status_upsert_query(
                item_hash=pending_message.item_hash,
                new_status=MessageStatus.PENDING,
                reception_time=reception_time,
                where=MessageStatusDb.status == MessageStatus.REJECTED,
            )
            insert_pending_message_stmt = insert(PendingMessageDb).values(
                pending_message.to_dict(exclude={"id"})
            )

            try:
                session.execute(upsert_message_status_stmt)
                session.execute(insert_pending_message_stmt)
                session.commit()
                return pending_message

            except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
                LOGGER.warning(
                    "Failed to add new pending message %s - DB error: %s",
                    pending_message.item_hash,
                    str(e),
                )
                session.rollback()
                reject_new_pending_message(
                    session=session,
                    pending_message=message_dict,
                    exception=e,
                )
                session.commit()
                return None

    async def insert_message(
        self, session: DbSession, pending_message: PendingMessageDb, message: MessageDb
    ):
        session.execute(make_message_upsert_query(message))
        if message.item_type != ItemType.inline:
            upsert_file(
                session=session,
                file_hash=message.item_hash,
                size=message.size,
                file_type=FileType.FILE,
            )
            insert_content_file_pin(
                session=session,
                file_hash=message.item_hash,
                owner=message.sender,
                item_hash=message.item_hash,
                created=timestamp_to_datetime(message.content["time"]),
            )

        delete_pending_message(session=session, pending_message=pending_message)
        session.execute(
            make_message_status_upsert_query(
                item_hash=message.item_hash,
                new_status=MessageStatus.PROCESSED,
                reception_time=pending_message.reception_time,
                where=(MessageStatusDb.status == MessageStatus.PENDING),
            )
        )

        if tx_hash := pending_message.tx_hash:
            session.execute(
                make_confirmation_upsert_query(
                    item_hash=message.item_hash, tx_hash=tx_hash
                )
            )

    async def verify_and_fetch(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> MessageDb:
        await self.verify_signature(pending_message=pending_message)
        validated_message = await self.fetch_pending_message(
            pending_message=pending_message
        )
        await self.fetch_related_content(session=session, message=validated_message)
        return validated_message

    async def process(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> MessageDb:
        existing_message = get_message_by_item_hash(
            session=session, item_hash=pending_message.item_hash
        )
        if existing_message:
            await self.confirm_existing_message(
                session=session,
                existing_message=existing_message,
                pending_message=pending_message,
            )
            return existing_message

        message = await self.verify_and_fetch(
            session=session, pending_message=pending_message
        )
        content_handler = self.get_content_handler(message.type)
        await content_handler.check_dependencies(session=session, message=message)
        await self.check_permissions(session=session, message=message)
        await self.insert_message(
            session=session, pending_message=pending_message, message=message
        )
        await content_handler.process(session=session, messages=[message])
        return message

    async def check_permissions(self, session: DbSession, message: MessageDb):
        content_handler = self.get_content_handler(message.type)
        await content_handler.check_permissions(session=session, message=message)

    # TODO: this method is only used in tests. Consider removing it.
    async def fetch_and_process_one_message_db(self, pending_message: PendingMessageDb):
        with self.session_factory() as session:
            await self.process(session=session, pending_message=pending_message)
            session.commit()
