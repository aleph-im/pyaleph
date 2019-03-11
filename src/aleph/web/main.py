import pkg_resources
import aiohttp_jinja2

from aiocache import cached, SimpleMemoryCache
from aiohttp import web, ClientSession

from aleph.web import app

app.router.add_static('/static/',
                      path=pkg_resources.resource_filename('aleph.web', 'static/'),
                      name='static')

@aiohttp_jinja2.template('index.html')
async def index(request):
    """Index of aleph.
    """

    return {}

app.router.add_get('/', index)
