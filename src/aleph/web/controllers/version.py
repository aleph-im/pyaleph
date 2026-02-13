from aiohttp import web

from aleph.version import __version__


async def version(request):
    """
    Get the current API version.

    ---
    summary: Get version
    tags:
      - Info
    responses:
      '200':
        description: Current version
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/VersionResponse'
    """

    response = web.json_response({"version": __version__})
    return response
