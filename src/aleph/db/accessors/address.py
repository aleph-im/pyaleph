from typing import Mapping, Optional, Sequence

from sqlalchemy import func, select
from sqlalchemy.sql import Select

from aleph.db.models.address import AddressStats, AddressTotalMessages
from aleph.schemas.addresses_query_params import SortBy
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortOrder


def find_matching_addresses(
    session: DbSession, address_contains: str, limit: int = 5000
):
    """
    Find addresses matching a substring pattern using trigram index on the materialized view.
    This ensures we get unique addresses with their total message counts.

    Args:
        session: Database session
        address_contains: Substring to search for in addresses (case-insensitive)
        limit: Maximum number of addresses to return

    Returns:
        List of matching addresses
    """
    pattern = f"%{address_contains}%"

    address_query = (
        select(AddressTotalMessages.address)
        .where(AddressTotalMessages.address.ilike(pattern))
        .limit(limit)
    )

    return session.execute(address_query).all()


def make_fetch_stats_address_query(
    addresses: Optional[Sequence[str]] = None,
    filters: Optional[Mapping[SortBy, int]] = None,  # Can use different SortBy types
    sort_by: SortBy = SortBy.messages,
    sort_order: SortOrder = SortOrder.DESCENDING,
    page: int = 1,
    per_page: int = 20,
) -> Select:
    """
    Make query for address statistics with pagination info.
    Returns a SQLAlchemy Select query.
    """

    # Base Query
    base_stmt = select(
        AddressStats.address.label("address"),
        func.coalesce(func.sum(AddressStats.nb_messages), 0).label("messages"),
        func.coalesce(
            func.sum(AddressStats.nb_messages).filter(AddressStats.type == "POST"), 0
        ).label("post"),
        func.coalesce(
            func.sum(AddressStats.nb_messages).filter(AddressStats.type == "AGGREGATE"),
            0,
        ).label("aggregate"),
        func.coalesce(
            func.sum(AddressStats.nb_messages).filter(AddressStats.type == "STORE"), 0
        ).label("store"),
        func.coalesce(
            func.sum(AddressStats.nb_messages).filter(AddressStats.type == "PROGRAM"), 0
        ).label("program"),
        func.coalesce(
            func.sum(AddressStats.nb_messages).filter(AddressStats.type == "INSTANCE"),
            0,
        ).label("instance"),
        func.coalesce(
            func.sum(AddressStats.nb_messages).filter(AddressStats.type == "FORGET"), 0
        ).label("forget"),
    ).group_by(AddressStats.address)

    # Filter by address (list)
    if addresses:
        base_stmt = base_stmt.where(AddressStats.address.in_(addresses))

    breakdown = base_stmt.subquery()
    stmt = select(breakdown)

    # Apply Filter on query
    if filters:
        for key, minimum in filters.items():
            stmt = stmt.where(getattr(breakdown.c, key.value.lower()) >= minimum)

    # Sort Query
    sort_column = getattr(breakdown.c, sort_by.value.lower())
    stmt = stmt.order_by(
        sort_column.asc() if sort_order == SortOrder.ASCENDING else sort_column.desc()
    )

    # Pagination
    if per_page:  # Do we want to return all matching result if requested ?
        stmt = stmt.limit(per_page).offset((page - 1) * per_page)

    return stmt
