from typing import List, Iterable

from sqlalchemy import select, update

from aleph.db.models import DelegationPermissionDb, BasePermissionDb
from aleph.types.db_session import DbSession
import datetime as dt


def get_permissions(
    session: DbSession,
    address: str,
    on_behalf_of: str,
    datetime: dt.datetime,
) -> Iterable[BasePermissionDb]:
    select_stmt = select(BasePermissionDb).where(
        (BasePermissionDb.owner == on_behalf_of)
        & (BasePermissionDb.address == address)
        & (datetime >= BasePermissionDb.valid_from)
        & (datetime < BasePermissionDb.valid_until)
    )
    return session.execute(select_stmt).scalars()


def has_delegation_permission(
    session: DbSession, address: str, on_behalf_of: str, datetime: dt.datetime
) -> bool:
    """
    Returns whether `address` has been given the permission to assign permissions
    for the `on_behalf_of` address.
    :param session: DB session.
    :param address: Address to check.
    :param on_behalf_of: Main address.
    :param datetime: Permission check datetime.
    """

    return DelegationPermissionDb.exists(
        session=session,
        where=(DelegationPermissionDb.owner == on_behalf_of)
        & (DelegationPermissionDb.address == address)
        & (datetime >= DelegationPermissionDb.valid_from)
        & (datetime < DelegationPermissionDb.valid_until),
    )


def expire_permissions(
    session: DbSession,
    address: str,
    granted_by: str,
    on_behalf_of: str,
    expiration_datetime: dt.datetime,
) -> None:
    update_stmt = (
        update(BasePermissionDb)
        .where(
            (BasePermissionDb.owner == on_behalf_of)
            & (BasePermissionDb.address == address)
            & (BasePermissionDb.granted_by == granted_by)
            & (BasePermissionDb.valid_until > expiration_datetime)
        )
        .values(expiration_datetime=expiration_datetime)
    )
    session.execute(update_stmt)
