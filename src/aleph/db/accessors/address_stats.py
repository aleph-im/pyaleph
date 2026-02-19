from typing import Optional

from sqlalchemy import func, select

from aleph.db.models.message_counts import MessageCountsDb
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageStatus


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


def count_address_stats(
    session: DbSession, address_contains: Optional[str] = None
) -> int:
    """
    Count the total number of unique addresses using the message_counts table.
    """
    base_stmt = (
        select(MessageCountsDb.sender)
        .where(
            MessageCountsDb.status == MessageStatus.PROCESSED.value,
            MessageCountsDb.owner == "",
            MessageCountsDb.sender != "",
            MessageCountsDb.type != "",
        )
        .group_by(MessageCountsDb.sender)
    )

    if address_contains:
        escaped_pattern = escape_like_pattern(address_contains)
        base_stmt = base_stmt.where(
            MessageCountsDb.sender.ilike(f"%{escaped_pattern}%", escape="\\")
        )

    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    return session.execute(count_stmt).scalar_one()
