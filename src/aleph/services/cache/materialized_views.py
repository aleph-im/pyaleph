import asyncio
import logging

from aleph.db.accessors.messages import refresh_address_stats_mat_view
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


async def refresh_cache_materialized_views(session_factory: DbSessionFactory) -> None:
    """
    Refresh DB materialized views used as caches, periodically.

    Materialized views are a simple solution to cache expensive DB queries, at the cost
    of refreshing them manually once in a while. This background task does exactly that.
    Note that materialized views used by the API should support concurrent refreshing
    to reduce latency.
    """

    while True:
        try:
            with session_factory() as session:
                refresh_address_stats_mat_view(session)
                session.commit()
                LOGGER.info("Refreshed address stats materialized view")

        except Exception:
            LOGGER.exception("Error refreshing cache materialized views")

        await asyncio.sleep(10 * 60)
