import asyncio
import logging

from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


async def refresh_cache_materialized_views(session_factory: DbSessionFactory) -> None:
    """
    Kept for backward compatibility. The address_stats materialized view
    has been replaced by the trigger-maintained message_counts table.
    """

    while True:
        await asyncio.sleep(10 * 60)
