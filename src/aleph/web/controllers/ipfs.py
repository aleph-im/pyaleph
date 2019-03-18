from aleph.storage import add_json, add_file
from aleph.web import app
from aiohttp import web


async def ipfs_add_json(request):
    """ Forward the json content to IPFS server and return an hash
    """
    data = await request.json()

    output = {
        'status': 'success',
        'hash': await add_json(data)
    }
    return web.json_response(output)

app.router.add_post('/api/v0/ipfs/add_json', ipfs_add_json)


async def ipfs_add_file(request):
    # No need to pin it here anymore.
    # TODO: find a way to specify linked ipfs hashes in posts/aggr.
    post = await request.post()
    output = await add_file(post['file'].file, post['file'].filename)

    output = {
        'status': 'success',
        'hash': output['Hash'],
        'name': output['Name'],
        'size': output['Size']
    }
    return web.json_response(output)

app.router.add_post('/api/v0/ipfs/add_file', ipfs_add_file)
