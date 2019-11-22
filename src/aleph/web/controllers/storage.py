from aleph.storage import add_json, get_hash_content
from aleph.web import app
from aleph.services.ipfs.pubsub import pub
from aiohttp import web
import base64
import asyncio


async def add_ipfs_json_controller(request):
    """ Forward the json content to IPFS server and return an hash
    """
    data = await request.json()

    output = {
        'status': 'success',
        'hash': await add_json(data, engine='ipfs')
    }
    return web.json_response(output)

app.router.add_post('/api/v0/ipfs/add_json', add_ipfs_json_controller)

async def add_storage_json_controller(request):
    """ Forward the json content to IPFS server and return an hash
    """
    data = await request.json()

    output = {
        'status': 'success',
        'hash': await add_json(data, engine='storage')
    }
    return web.json_response(output)

app.router.add_post('/api/v0/storage/add_json', add_storage_json_controller)

def prepare_content(content):
    return base64.encodebytes(content).decode('utf-8')

async def get_hash(request):
    result = {'status': 'error',
              'reason': 'unknown'}
    item_hash = request.match_info.get('hash', None)
    
    if hash is not None:
        value = await get_hash_content(item_hash, use_network=False)
        loop = asyncio.get_event_loop()
        content = await loop.run_in_executor(None, prepare_content, value)
    
        if value is not None and value != -1:
            result = {'status': 'success',
                      'hash': item_hash,
                      'content': content}
        else:
            result = {'status': 'success',
                      'hash': item_hash,
                      'content': None}
    else:
        result = {'status': 'error',
                'reason': 'no hash provided'}
        
    response = web.json_response(result)
    response.enable_compression()
    return response

    
app.router.add_get('/api/v0/storage/{hash}', get_hash)