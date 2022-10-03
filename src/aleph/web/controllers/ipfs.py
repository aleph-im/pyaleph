from aiohttp import web

from aleph.services.ipfs import IpfsService


async def ipfs_add_file(request):
    ipfs_service: IpfsService = request.app["storage_service"].ipfs_service

    # No need to pin it here anymore.
    # TODO: find a way to specify linked ipfs hashes in posts/aggr.
    post = await request.post()
    output = await ipfs_service.add_file(post["file"].file)

    output = {
        "status": "success",
        "hash": output["Hash"],
        "name": output["Name"],
        "size": output["Size"],
    }
    return web.json_response(output)
