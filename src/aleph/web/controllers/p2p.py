from aleph.services.ipfs.storage import add_file
from aleph.storage import add_json
from aleph.web import app
from aleph.services.ipfs.pubsub import pub as pub_ipfs
from aleph.services.p2p import pub as pub_p2p
from aiohttp import web
import logging

LOGGER = logging.getLogger('web.controllers.p2p')

async def pub_json(request):
    """ Forward the message to P2P host and IPFS server as a pubsub message
    """
    data = await request.json()
    status = "success"
    try:
        if app['config'].ipfs.enabled.value:
            await pub_ipfs(data.get('topic'), data.get('data'))
    except Exception:
        LOGGER.exception("Can't publish on ipfs")
        status = "warning"
    
    
    try:
        await pub_p2p(data.get('topic'), data.get('data'))
    except Exception:
        LOGGER.exception("Can't publish on p2p")
        status = "warning"

    output = {
        'status': status
    }
    return web.json_response(output)

app.router.add_post('/api/v0/ipfs/pubsub/pub', pub_json)
app.router.add_post('/api/v0/p2p/pubsub/pub', pub_json)
