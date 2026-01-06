from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.sql import Select

from aleph.db.models.address import AddressStats
from aleph.schemas.addresses_query_params import SortBy
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortOrder


def make_address_filter_subquery(address_contains: str):
    """
    Subquery defining the set of addresses to include.
    Only used when address filtering is requested.
    """
    return (
        select(AddressStats.address)
        .distinct()
        .where(func.lower(AddressStats.address).contains(address_contains.lower()))
        .subquery()
    )


def make_fetch_stats_address_query(
    address_contains: Optional[str] = None,
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

    if address_contains:
        address_subquery = make_address_filter_subquery(address_contains)
        base_stmt = base_stmt.join(
            address_subquery, AddressStats.address == address_subquery.c.address
        )

    breakdown = base_stmt.subquery()
    stmt = select(breakdown)

    sort_column = getattr(breakdown.c, sort_by.value.lower())
    stmt = stmt.order_by(
        sort_column.asc() if sort_order == SortOrder.ASCENDING else sort_column.desc(),
        breakdown.c.address.asc(),
    )

    # Pagination
    if per_page:  # Return all matching results if per_page is 0
        stmt = stmt.limit(per_page).offset((page - 1) * per_page)

    return stmt


def count_address_stats(
    session: DbSession, address_contains: Optional[str] = None
) -> int:
    """
    Count the total number of unique addresses in the address stats view.

    Args:
        session: Database session
        address_contains: Optional substring filter for addresses

    Returns:
        Total count of unique addresses matching the filter
    """
    # Get the same base query as used in make_fetch_stats_address_query
    base_stmt = select(AddressStats.address).group_by(AddressStats.address)

    # Apply filter if address_contains is provided
    if address_contains:
        base_stmt = base_stmt.where(
            func.lower(AddressStats.address).contains(address_contains.lower())
        )

    # Count the total number of addresses
    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    return session.execute(count_stmt).scalar_one()
