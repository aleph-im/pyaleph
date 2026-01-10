from typing import Optional

from sqlalchemy import func, select

from aleph.db.models.address_stats import AddressStats
from aleph.types.db_session import DbSession


def escape_like_pattern(pattern: str) -> str:
    """
    Escape SQL LIKE/ILIKE wildcard characters to prevent pattern injection.

    This function escapes the special characters %, _, and \\ that have special
    meaning in SQL LIKE patterns, preventing users from injecting wildcards
    that could enumerate or match unintended data.

    Args:
        pattern: The user-provided search pattern

    Returns:
        The pattern with LIKE special characters escaped using backslash
    """
    # Escape backslash first (since it's the escape character)
    # then escape % and _
    return pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def make_address_filter_subquery(address_contains: str):
    """
    Subquery defining the set of addresses to include.
    Only used when address filtering is requested.
    """
    escaped_pattern = escape_like_pattern(address_contains.lower())
    return (
        select(AddressStats.address)
        .distinct()
        .where(
            func.lower(AddressStats.address).like(f"%{escaped_pattern}%", escape="\\")
        )
        .subquery()
    )


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
        address_subquery = make_address_filter_subquery(address_contains)
        base_stmt = base_stmt.join(
            address_subquery, AddressStats.address == address_subquery.c.address
        )

    # Count the total number of addresses
    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    return session.execute(count_stmt).scalar_one()
