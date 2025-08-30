from __future__ import annotations

import logging
from typing import Dict, List, Sequence, Set, cast

from aleph_message.models import ForgetContent, ItemHash, MessageType
from sqlalchemy import select

from aleph.db.accessors.aggregates import aggregate_exists
from aleph.db.accessors.messages import (
    append_to_forgotten_by,
    forget_message,
    get_message_by_item_hash,
    get_message_status,
    message_exists,
)
from aleph.db.accessors.vms import get_vms_dependent_volumes
from aleph.db.models import AggregateElementDb, MessageDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.types.db_session import AsyncDbSession
from aleph.types.message_status import (
    CannotForgetForgetMessage,
    ForgetNotAllowed,
    ForgetTargetNotFound,
    InternalError,
    MessageStatus,
    NoForgetTarget,
    PermissionDenied,
)

logger = logging.getLogger(__name__)


class ForgetMessageHandler(ContentHandler):
    def __init__(
        self,
        content_handlers: Dict[MessageType, ContentHandler],
    ):

        self.content_handlers = content_handlers
        self.content_handlers[MessageType.forget] = self

    async def check_dependencies(
        self, session: AsyncDbSession, message: MessageDb
    ) -> None:
        """
        We only consider FORGETs as fetched if the messages / aggregates they target
        already exist. Otherwise, we retry them later.
        """

        content = message.parsed_content
        message_item_hash = message.item_hash

        logger.debug(f"{message_item_hash}: checking for forget message targets")
        assert isinstance(content, ForgetContent)

        if not content.hashes and not content.aggregates:
            # The user did not specify anything to forget.
            raise NoForgetTarget()

        for item_hash in content.hashes:
            if not await message_exists(session=session, item_hash=item_hash):
                raise ForgetTargetNotFound(item_hash)

            # Check file references, on VM volumes, as data volume and as code volume
            # to block the deletion if we found ones
            dependent_volumes = await get_vms_dependent_volumes(
                session=session, volume_hash=item_hash
            )
            if dependent_volumes is not None:
                raise ForgetNotAllowed(
                    file_hash=item_hash, vm_hash=dependent_volumes.item_hash
                )

        for aggregate_key in content.aggregates:
            if not await aggregate_exists(
                session=session, key=aggregate_key, owner=content.address
            ):
                raise ForgetTargetNotFound(aggregate_key=aggregate_key)

    @staticmethod
    async def _list_target_messages(
        session: AsyncDbSession, forget_message: MessageDb
    ) -> Sequence[ItemHash]:
        content = cast(ForgetContent, forget_message.parsed_content)

        aggregate_messages_to_forget: List[ItemHash] = []
        for aggregate in content.aggregates:
            # TODO: write accessor
            aggregate_messages_to_forget.extend(
                ItemHash(value)
                for value in (
                    await session.execute(
                        select(AggregateElementDb.item_hash).where(
                            (AggregateElementDb.key == aggregate)
                            & (AggregateElementDb.owner == content.address)
                        )
                    )
                ).scalars()
            )

        return content.hashes + aggregate_messages_to_forget

    async def check_permissions(self, session: AsyncDbSession, message: MessageDb):
        await super().check_permissions(session=session, message=message)

        # Check that the sender owns the objects it is attempting to forget
        target_hashes = await self._list_target_messages(
            session=session, forget_message=message
        )
        for target_hash in target_hashes:
            target_status = await get_message_status(
                session=session, item_hash=target_hash
            )
            if not target_status:
                raise ForgetTargetNotFound(target_hash=target_hash)

            if target_status.status in (
                MessageStatus.FORGOTTEN,
                MessageStatus.REJECTED,
                MessageStatus.REMOVED,
            ):
                continue

            # Note: Only allow to forget messages that are processed or marked for removing
            if (
                target_status.status != MessageStatus.PROCESSED
                and target_status.status != MessageStatus.REMOVING
            ):
                raise ForgetTargetNotFound(target_hash=target_hash)

            target_message = await get_message_by_item_hash(
                session=session, item_hash=target_hash
            )
            if not target_message:
                raise InternalError(
                    f"Target message {target_hash} is marked as processed but does not exist."
                )
            if target_message.type == MessageType.forget:
                logger.warning(
                    "FORGET message %s may not forget FORGET message %s",
                    message.item_hash,
                    target_hash,
                )
                raise CannotForgetForgetMessage(target_hash)
            if target_message.sender != message.sender:
                raise PermissionDenied(
                    f"Cannot forget message {target_hash} because it belongs to another user"
                )

    async def _forget_by_message_type(
        self, session: AsyncDbSession, message: MessageDb
    ) -> Set[str]:
        """
        When processing a FORGET message, performs additional cleanup depending
        on the type of message that is being forgotten.
        """
        content_handler = self.content_handlers[message.type]
        return await content_handler.forget_message(session=session, message=message)

    async def _forget_message(
        self, session: AsyncDbSession, message: MessageDb, forgotten_by: MessageDb
    ):
        # Mark the message as forgotten
        await forget_message(
            session=session,
            item_hash=message.item_hash,
            forget_message_hash=forgotten_by.item_hash,
        )

        additional_messages_to_forget = await self._forget_by_message_type(
            session=session, message=message
        )
        for item_hash in additional_messages_to_forget:
            await forget_message(
                session=session,
                item_hash=item_hash,
                forget_message_hash=forgotten_by.item_hash,
            )

    async def _forget_item_hash(
        self, session: AsyncDbSession, item_hash: str, forgotten_by: MessageDb
    ):
        message_status = await get_message_status(
            session=session, item_hash=ItemHash(item_hash)
        )
        if not message_status:
            raise ForgetTargetNotFound(target_hash=item_hash)

        if message_status.status == MessageStatus.REJECTED:
            logger.info("Message %s was rejected, nothing to do.", item_hash)
        if message_status.status == MessageStatus.REMOVED:
            logger.info("Message %s was removed, nothing to do.", item_hash)
        if message_status.status == MessageStatus.FORGOTTEN:
            logger.info("Message %s is already forgotten, nothing to do.", item_hash)
            await append_to_forgotten_by(
                session=session,
                forgotten_message_hash=item_hash,
                forget_message_hash=forgotten_by.item_hash,
            )
            return

        # Note: Only allow to forget messages that are processed or marked for removing
        if (
            message_status.status != MessageStatus.PROCESSED
            and message_status.status != MessageStatus.REMOVING
        ):
            logger.error(
                "FORGET message %s targets message %s which is not processed yet. This should not happen.",
                forgotten_by.item_hash,
                item_hash,
            )
            raise ForgetTargetNotFound(item_hash)

        message = await get_message_by_item_hash(
            session=session, item_hash=ItemHash(item_hash)
        )
        if not message:
            raise ForgetTargetNotFound(item_hash)

        if message.type == MessageType.forget:
            # This should have been detected in check_permissions(). Raise an exception
            # if it happens nonetheless as it indicates an unforeseen concurrent modification
            # of the database.
            raise CannotForgetForgetMessage(message.item_hash)

        await self._forget_message(
            session=session,
            message=message,
            forgotten_by=forgotten_by,
        )

    async def _process_forget_message(
        self, session: AsyncDbSession, message: MessageDb
    ):

        hashes_to_forget = await self._list_target_messages(
            session=session, forget_message=message
        )

        for item_hash in hashes_to_forget:
            await self._forget_item_hash(
                session=session, item_hash=item_hash, forgotten_by=message
            )

    async def process(self, session: AsyncDbSession, messages: List[MessageDb]) -> None:

        # FORGET:
        # 0. Check permissions: separate step now
        # 1. Check if the message is already forgotten -> if yes, add to forgotten_by and done
        # 2. Get all the messages to forget, including aggregates
        # 3. Forget the messages
        # 4. For each type of message, perform an additional check

        for message in messages:
            await self._process_forget_message(session=session, message=message)

    async def forget_message(
        self, session: AsyncDbSession, message: MessageDb
    ) -> Set[str]:
        raise CannotForgetForgetMessage(target_hash=message.item_hash)
