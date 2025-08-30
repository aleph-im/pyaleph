from typing import List

from aiocache import SimpleMemoryCache, cached
from aiohttp import web

from aleph.db.accessors.messages import get_distinct_channels
from aleph.types.channel import Channel
from aleph.types.db_session import AsyncDbSession
from aleph.web.controllers.app_state_getters import get_session_factory_from_request


@cached(ttl=60 * 120, cache=SimpleMemoryCache, timeout=120)
async def get_channels(session: AsyncDbSession) -> List[Channel]:
    channels = await get_distinct_channels(session)
    return list(channels)


async def used_channels(request: web.Request) -> web.Response:
    """All used channels list

    TODO: do we need pagination?
    """

    session_factory = get_session_factory_from_request(request)

    async with session_factory() as session:
        channels = await get_channels(session)

    response = web.json_response({"channels": channels})
    response.enable_compression()
    return response
