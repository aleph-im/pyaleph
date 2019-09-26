from aleph.services.ipfs.storage import add_file
from aleph.storage import add_json
from aleph.web import app
from aleph.services.ipfs.pubsub import pub
from aiohttp import web


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