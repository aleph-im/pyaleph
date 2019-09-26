from aleph.storage import add_json
from aleph.web import app
from aleph.services.ipfs.pubsub import pub
from aiohttp import web


async def add_json_controller(request):
    """ Forward the json content to IPFS server and return an hash
    """
    data = await request.json()

    output = {
        'status': 'success',
        'hash': await add_json(data)
    }
    return web.json_response(output)

app.router.add_post('/api/v0/storage/add_json', add_json_controller)
app.router.add_post('/api/v0/ipfs/add_json', add_json_controller)