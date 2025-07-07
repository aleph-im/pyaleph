import asyncio

from aiohttp import web
from aiohttp.web_request import FileField

from aleph.db.accessors.files import upsert_file
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.utils import add_grace_period_for_file


async def ipfs_add_file(request: web.Request):
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

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

    file_content: bytes
    if isinstance(file_field, bytes):
        file_content = file_field
    elif isinstance(file_field, str):
        file_content = file_field.encode()
    elif isinstance(file_field, FileField):
        if file_field.content_type != "application/octet-stream":
            raise web.HTTPUnprocessableEntity(
                reason="Invalid content-type for 'file' field. Must be 'application/octet-stream'."
            )
        file_content = file_field.file.read()
    else:
        raise web.HTTPUnprocessableEntity(
            reason="Invalid type for 'file' field. Must be bytes, str or FileField."
        )

    ipfs_add_response = await ipfs_service.add_file(file_content)

    cid = ipfs_add_response["Hash"]
    name = ipfs_add_response["Name"]

    # IPFS add returns the cumulative size and not the real file size.
    # We need the real file size here.
    stats = await asyncio.wait_for(
        ipfs_service.ipfs_client.files.stat(f"/ipfs/{cid}"), 5
    )
    size = stats["Size"]

    async with session_factory() as session:
        await upsert_file(
            session=session,
            file_hash=cid,
            size=size,
            file_type=FileType.FILE,
        )
        await add_grace_period_for_file(
            session=session, file_hash=cid, hours=grace_period
        )
        await session.commit()

    output = {
        "status": "success",
        "hash": cid,
        "name": name,
        "size": size,
    }
    return web.json_response(output)
