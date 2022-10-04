from typing import List

from aiocache import cached, SimpleMemoryCache
from aiohttp import web

from aleph.db.accessors.messages import get_distinct_channels
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory, DbSession


@cached(ttl=60 * 120, cache=SimpleMemoryCache, timeout=120)
async def get_channels(session: DbSession) -> List[Channel]:
    channels = get_distinct_channels(session)
    return list(channels)


async def used_channels(request):
    """All used channels list

    TODO: do we need pagination?
    """

    session_factory: DbSessionFactory = request.app["session_factory"]

    with session_factory() as session:
        channels = await get_channels(session)

    response = web.json_response({"channels": channels})
    response.enable_compression()
    return response
