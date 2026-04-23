import asyncio
import logging

from aiohttp import BodyPartReader, web

from aleph.db.accessors.files import upsert_file
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.storage import MultipartUploadedFile
from aleph.web.controllers.utils import add_grace_period_for_file

logger = logging.getLogger(__name__)


async def ipfs_add_file(request: web.Request):
    """
    Upload a file to IPFS.

    ---
    summary: Add file to IPFS
    tags:
      - IPFS
    requestBody:
      required: true
      content:
        multipart/form-data:
          schema:
            type: object
            required:
              - file
            properties:
              file:
                type: string
                format: binary
              metadata:
                type: string
                description: Optional JSON with a signed STORE message.
    responses:
      '200':
        description: Upload result with IPFS CID
      '403':
        description: IPFS is disabled on this node, or signature invalid
      '413':
        description: File too large
      '422':
        description: Invalid multipart or metadata
    """
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value
    max_unauthenticated_upload_file_size = (
        config.ipfs.max_unauthenticated_upload_file_size.value
    )

    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)

    uploaded_file = None
    filename = "file"
    try:
        if request.content_type != "multipart/form-data":
            raise web.HTTPBadRequest(
                reason="Expected Content-Type: multipart/form-data"
            )

        reader = await request.multipart()
        async for part in reader:
            if part is None:
                raise web.HTTPBadRequest(reason="Invalid multipart structure")
            if not isinstance(part, BodyPartReader):
                raise web.HTTPBadRequest(reason="Invalid multipart structure")

            if part.name == "file":
                filename = part.filename or "file"
                uploaded_file = MultipartUploadedFile(
                    part, max_unauthenticated_upload_file_size
                )
                await uploaded_file.read_and_validate()

        if uploaded_file is None:
            raise web.HTTPUnprocessableEntity(
                reason="Missing 'file' in multipart form."
            )

        temp_file = await uploaded_file.open_temp_file()
        file_content = await temp_file.read()
        if isinstance(file_content, str):
            file_content = file_content.encode("utf-8")

        cid = await ipfs_service.add_bytes(file_content)

        try:
            stats = await asyncio.wait_for(
                ipfs_service.pinning_client.files.stat(f"/ipfs/{cid}"),
                config.ipfs.stat_timeout.value,
            )
            size = stats["Size"]
        except TimeoutError:
            raise web.HTTPNotFound(reason="File not found on IPFS")

        with session_factory() as session:
            upsert_file(
                session=session,
                file_hash=cid,
                size=size,
                file_type=FileType.FILE,
            )
            add_grace_period_for_file(
                session=session, file_hash=cid, hours=grace_period
            )
            session.commit()

        return web.json_response(
            {
                "status": "success",
                "hash": cid,
                "name": filename,
                "size": size,
            }
        )

    finally:
        if uploaded_file is not None:
            await uploaded_file.cleanup()
