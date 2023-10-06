import asyncio

from aiohttp import web

from aleph.db.accessors.files import upsert_file
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_ipfs_service_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.utils import file_field_to_io


async def ipfs_add_file(request: web.Request):
    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)

    # No need to pin it here anymore.
    post = await request.post()
    try:
        file_field = post["file"]
    except KeyError:
        raise web.HTTPUnprocessableEntity(reason="Missing 'file' in multipart form.")

    ipfs_add_response = await ipfs_service.add_file(file_field_to_io(file_field))

    cid = ipfs_add_response["Hash"]
    name = ipfs_add_response["Name"]

    # IPFS add returns the cumulative size and not the real file size.
    # We need the real file size here.
    stats = await asyncio.wait_for(ipfs_service.ipfs_client.files.stat(f"/ipfs/{cid}"), 5)
    size = stats["Size"]

    with session_factory() as session:
        upsert_file(
            session=session,
            file_hash=cid,
            size=size,
            file_type=FileType.FILE,
        )
        session.commit()

    output = {
        "status": "success",
        "hash": cid,
        "name": name,
        "size": size,
    }
    return web.json_response(output)
