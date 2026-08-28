from typing import List

from aiocache import SimpleMemoryCache, cached
from aiohttp import web

from aleph.db.accessors.messages import get_distinct_channels
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession
from aleph.web.controllers.app_state_getters import get_session_factory_from_request


@cached(
    ttl=60 * 120,
    cache=SimpleMemoryCache,
    timeout=120,
    # aiocache calls key_builder(f, *args, **kwargs); we ignore all of them and
    # return a constant key. The channel list does not depend on which request
    # Session is passed. Without this, aiocache's default key includes the
    # per-request Session's repr (its memory address), so every request is a
    # cache miss that stores a new entry — the cache never hits and grows with
    # traffic until TTL eviction.
    key_builder=lambda *args, **kwargs: "used_channels",
)
async def get_channels(session: DbSession) -> List[Channel]:
    # Filter out None
    return [
        channel
        for channel in get_distinct_channels(session=session)
        if channel is not None
    ]


async def used_channels(request: web.Request) -> web.Response:
    """
    List all used channels.

    ---
    summary: List channels
    tags:
      - Channels
    responses:
      '200':
        description: List of channels
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ChannelsResponse'
    """

    session_factory = get_session_factory_from_request(request)

    with session_factory() as session:
        channels = await get_channels(session)

    response = web.json_response({"channels": channels})
    response.enable_compression()
    return response
