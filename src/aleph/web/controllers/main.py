import asyncio

import pkg_resources
import aiohttp_jinja2
from aiohttp import web

from aleph.web import app
from aleph import __version__
import aleph.model


async def get_status():
    return {
        'messages': await aleph.model.db.messages.count_documents({}),
        'pending_messages': await aleph.model.db.pending_messages.count_documents({}),
    }


app.router.add_static('/static/',
                      path=pkg_resources.resource_filename('aleph.web',
                                                           'static/'),
                      name='static')


@aiohttp_jinja2.template('index.html')
async def index(request):
    """Index of aleph.
    """
    return await get_status()

app.router.add_get('/', index)

async def version(request):
    """Version endpoint.
    """
    
    response = web.json_response({
        "version": __version__
        })
    return response
app.router.add_get('/version', version)
app.router.add_get('/api/v0/version', version)


async def status_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    previous_status = None
    while True:
        status = await get_status()

        if status != previous_status:
            await ws.send_json(status)
            previous_status = status

        await asyncio.sleep(0.5)


app.router.add_get('/api/ws0/status', status_ws)
