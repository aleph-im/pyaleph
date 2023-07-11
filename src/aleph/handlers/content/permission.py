""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- handle garbage collection of unused hashes
"""

import logging
from copy import deepcopy
from typing import List, Set, Tuple

from aleph.db.accessors.permissions import (
    has_delegation_permission,
    get_permissions,
    expire_permissions,
)
from aleph.db.models import (
    MessageDb,
    BasePermissionDb,
    DelegationPermissionDb,
    PostPermissionDb,
    ExecutablePermissionDb,
    AggregatePermissionDb,
    StorePermissionDb,
)
from aleph.handlers.content.content_handler import ContentHandler
from aleph.schemas.permissions import (
    PermissionContent,
    DelegationPermission,
    Permission,
    PostPermission,
    AggregatePermission,
    StorePermission,
    ExecutablePermission,
    CrudPermission,
)
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.message_status import (
    PermissionDenied,
    InvalidMessageFormat,
    CannotForgetPermissionMessage,
    PermissionCannotDelegateDelegation,
)

LOGGER = logging.getLogger(__name__)


def _get_permission_content(message: MessageDb) -> PermissionContent:
    content = message.parsed_content
    if not isinstance(content, PermissionContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for permission message: {message.item_hash}"
        )
    return content


PERMISSION_TYPE_MAP = {
    AggregatePermission: AggregatePermissionDb,
    DelegationPermission: DelegationPermissionDb,
    ExecutablePermission: ExecutablePermissionDb,
    PostPermission: PostPermissionDb,
    StorePermission: StorePermissionDb,
}


def map_permission_to_db(
    permission: Permission,
    content: PermissionContent,
    address: str,
    message: MessageDb,
) -> BasePermissionDb:
    db_model_args = {
        "owner": content.address,
        "granted_by": message.sender,
        "address": address,
        "channel": message.channel,
        "valid_from": timestamp_to_datetime(content.time),
        "valid_until": None,
        "expires": None,
    }

    if isinstance(permission, CrudPermission):
        db_model_args["create"] = permission.create
        db_model_args["update"] = permission.update
        db_model_args["delete"] = permission.delete
        db_model_args["refs"] = permission.refs
        db_model_args["addresses"] = permission.addresses

    if isinstance(permission, PostPermission):
        db_model_args["post_types"] = permission.post_types

    return PERMISSION_TYPE_MAP[type(permission)](**db_model_args)


PermissionSet = Set[BasePermissionDb]


def get_permission_diff(
    current_permissions: PermissionSet, new_permissions: PermissionSet
) -> Tuple[PermissionSet, PermissionSet, PermissionSet]:

    # Remove identical permissions
    permissions_to_add = new_permissions - current_permissions
    permissions_to_keep = current_permissions & new_permissions
    permissions_to_expire = current_permissions - new_permissions

    return permissions_to_keep, permissions_to_add, permissions_to_expire


def make_new_delegated_permission(
    delegated_permission: BasePermissionDb, replaced_by: BasePermissionDb
) -> BasePermissionDb:
    new_delegated_permission = deepcopy(replaced_by)
    new_delegated_permission.address = delegated_permission.address
    new_delegated_permission.granted_by = delegated_permission.granted_by
    return new_delegated_permission


def update_delegated_permissions(
    permissions_to_expire: PermissionSet, new_permissions: PermissionSet
) -> None:
    for current_permission in permissions_to_expire:
        for new_permission in new_permissions:
            if new_permission.is_subset(current_permission):
                updated_delegations = [
                    make_new_delegated_permission(
                        delegated_permission=delegation, replaced_by=new_permission
                    )
                    for delegation in current_permission.delegations
                ]
                new_permission.delegations += updated_delegations


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

            new_permissions_set = set(
                map_permission_to_db(
                    permission=permission,
                    content=content,
                    address=address,
                    message=message,
                )
                for permission in permissions
            )

            # Isolate new permissions (diff DB vs message).
            (
                permissions_to_keep,
                permissions_to_add,
                permissions_to_expire,
            ) = get_permission_diff(
                current_permissions=set(current_permissions),
                new_permissions=new_permissions_set,
            )

            # Nothing to do, move on to the next address.
            if not (permissions_to_expire or permissions_to_add):
                continue

            # If message deletes delegation permission, expire all delegations.
            if any(
                isinstance(permission, DelegationPermission)
                for permission in permissions_to_expire
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
                update_delegated_permissions(
                    permissions_to_expire=permissions_to_expire,
                    new_permissions=permissions_to_add,
                )

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
