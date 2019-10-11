import pkg_resources
import aiohttp_jinja2
from aiohttp import web

from aleph.web import app
from aleph import __version__

app.router.add_static('/static/',
                      path=pkg_resources.resource_filename('aleph.web',
                                                           'static/'),
                      name='static')


@aiohttp_jinja2.template('index.html')
async def index(request):
    """Index of aleph.
    """

    return {}

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