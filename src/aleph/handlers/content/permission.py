""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- handle garbage collection of unused hashes
"""

import asyncio
import logging
from typing import List, Optional, Set, Tuple

import aioipfs
from aioipfs import NotPinnedError
from aioipfs.api import RepoAPI
from aleph_message.models import ItemType, StoreContent, ItemHash

from aleph.config import get_config
from aleph.db.accessors.files import (
    delete_file as delete_file_db,
    insert_message_file_pin,
    get_file_tag,
    upsert_file_tag,
    delete_file_pin,
    refresh_file_tag,
    is_pinned_file,
    get_message_file_pin,
    upsert_file,
)
from aleph.db.accessors.permissions import (
    has_delegation_permission,
    get_permissions,
    expire_permissions,
)
from aleph.db.models import MessageDb, BasePermissionDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.handlers.content.content_handler import ContentHandler
from aleph.schemas.permissions import PermissionContent, DelegationPermission
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import (
    PermissionDenied,
    FileUnavailable,
    InvalidMessageFormat,
    StoreRefNotFound,
    StoreCannotUpdateStoreWithRef,
    CannotForgetForgetMessage,
    CannotForgetPermissionMessage,
    PermissionCannotDelegateDelegation,
)
from aleph.utils import item_type_from_hash

LOGGER = logging.getLogger(__name__)


def _get_permission_content(message: MessageDb) -> PermissionContent:
    content = message.parsed_content
    if not isinstance(content, PermissionContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for permission message: {message.item_hash}"
        )
    return content


def _get_permission_diff(
    current_permissions: Set[BasePermissionDb], new_permissions: Set[BasePermissionDb]
) -> Tuple[Set[BasePermissionDb], Set[BasePermissionDb]]:

    permissions_to_keep = set()
    permissions_with_new_validity_range = set()
    permissions_to_add = set()
    permissions_to_expire = set()

    # Remove identical permissions
    for new_permission in new_permissions:
        for current_permission in current_permissions:
            if new_permission == current_permission:
                permissions_to_keep.add(current_permission)




            elif new_permission.is_subset(current_permission):
                if current_permission.children:
                    for child in current_permission.children:
                        if not child.is_subset(new_permission):
                            permissions_to_expire.add(child)

                    permissions_to_expire |= set(current_permission.children)
                    new_permission.children = []

                permissions_to_add.add(new_permission)
            else:
                permissions_to_add.add(new_permission)

    permissions_to_expire |= (current_permissions - permissions_to_keep)

    return permissions_to_expire, permissions_to_add





class PermissionMessageHandler(ContentHandler):
    def __init__(self, storage_service: StorageService):
        self.storage_service = storage_service

    async def check_permissions(self, session: DbSession, message: MessageDb) -> None:
        await super().check_permissions(session=session, message=message)
        content = _get_permission_content(message)
        sender = message.sender
        on_behalf_of = content.address
        message_datetime = timestamp_to_datetime(content.time)

        if on_behalf_of in content.permissions:
            raise PermissionDenied("An address cannot assign permissions to itself")

        if sender == on_behalf_of:
            return

        if not has_delegation_permission(
            session=session,
            address=sender,
            on_behalf_of=on_behalf_of,
            datetime=message_datetime,
        ):
            raise PermissionDenied(
                f"Address {on_behalf_of} did not allow {sender} to create permissions"
            )

        # Check if the permissions the address is trying to grant are a subset of
        # the permissions granted by the main address.
        sender_permissions = get_permissions(
            session=session,
            address=sender,
            on_behalf_of=on_behalf_of,
            datetime=message_datetime,
        )
        for address, permissions in content.permissions.items():
            for permission in permissions:
                if isinstance(permission, DelegationPermission):
                    raise PermissionCannotDelegateDelegation(
                        "Cannot delegate delegation permission"
                    )
                if not is_subset(permission, sender_permissions):
                    raise PermissionDenied(
                        f"Address {sender} is not authorized to "
                        f"delegate {permission} on behalf of {on_behalf_of}"
                    )

    async def process_permission_message(self, session: DbSession, message: MessageDb):
        content = _get_permission_content(message)
        granted_by = message.sender
        on_behalf_of = content.address
        message_datetime = timestamp_to_datetime(content.time)

        for address, permissions in content.permissions.items():
            current_permissions = get_permissions(
                session=session,
                address=address,
                on_behalf_of=on_behalf_of,
                datetime=message_datetime,
            )

            # Isolate new permissions (diff DB vs message).
            expired_permissions, new_permissions = _get_permission_diff(
                current_permissions, permissions
            )

            # Nothing to do, move on to the next address.
            if not (expired_permissions or new_permissions):
                continue

            # If message deletes delegation permission, expire all delegations.
            if any(
                isinstance(permission, DelegationPermission)
                for permission in expired_permissions
            ):
                ...
                # expire_permissions(session=session, address=)

            # If message still has delegation permission but changes other permissions, check the current
            # delegations and expire the ones that are not valid anymore (invalid subset delegated)?
            # Or simply enforce it for future delegations. TBD.

            # Expire invalid permissions, insert new ones.

            if has_delegation_permission(
                session=session,
                address=address,
                on_behalf_of=on_behalf_of,
                datetime=message_datetime,
            ):
                if any(
                    isinstance(permission, DelegationPermission)
                    for permission in permissions
                ):
                    # Do nothing? We need to check if the permissions granted to the address are still
                    # a superset of all the permissions that are delegated to the address, or expire them
                    # if that's not the case.
                    ...

            expire_permissions(
                session=session,
                granted_by=granted_by,
                on_behalf_of=on_behalf_of,
                address=address,
                expiration_datetime=message_datetime,
            )
            permissions_db = [
                map_permission(
                    permission=permission,
                    sender=message.sender,
                    on_behalf_of=content.address,
                )
                for permission in content.permissions
            ]

            insert_permissions(session=session, permissions=permissions_db)

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            await self.process_permission_message(session=session, message=message)

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        raise CannotForgetPermissionMessage(target_hash=message.item_hash)
