from aiohttp import web
from aiocache import cached, SimpleMemoryCache
from aleph.web import app
from aleph.model.messages import Message

@cached(ttl=60*120, cache=SimpleMemoryCache, timeout=120)
async def get_channels():
    values = await Message.collection.distinct('channel')
    return sorted([v for v in values if isinstance(v, str)])
    

async def used_channels(request):
    """ All used channels list
    
    TODO: do we need pagination?
    """
    
    response = web.json_response({
        'channels': await get_channels()
    })
    response.enable_compression()
    return response

app.router.add_get('/api/v0/channels/list.json', used_channels)

