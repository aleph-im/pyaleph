from aiohttp import web

from aleph.version import __version__


async def version(request):
    """Version endpoint."""

    response = web.json_response({"version": __version__})
    return response
