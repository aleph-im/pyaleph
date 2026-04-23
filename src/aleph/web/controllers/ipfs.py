import asyncio
import logging
import math

from aiohttp import BodyPartReader, web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType
from pydantic import ValidationError

from aleph.db.accessors.files import upsert_file
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.toolkit.constants import MiB
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_session_factory_from_request,
    get_signature_verifier_from_request,
)
from aleph.web.controllers.storage import (
    MultipartUploadedFile,
    StorageMetadata,
    _verify_message_signature,
    _verify_user_balance,
)
from aleph.web.controllers.utils import (
    add_grace_period_for_file,
    broadcast_and_process_message,
    broadcast_status_to_http_status,
)

logger = logging.getLogger(__name__)


async def ipfs_add_file(request: web.Request):
    """
    Upload a file to IPFS. Optionally include a signed STORE message so
    the upload is anchored to the aleph.im network in one call.

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
                description: >
                  Optional JSON with a signed STORE message
                  (item_type=ipfs). When present, the CID computed after
                  pinning must match message.content.item_hash.
    responses:
      '200':
        description: Upload result with IPFS CID
      '402':
        description: Insufficient balance for the STORE message
      '403':
        description: IPFS disabled on this node, or signature invalid
      '413':
        description: File too large
      '422':
        description: Invalid multipart, metadata, or CID mismatch
    """
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value
    max_upload_file_size = config.ipfs.max_upload_file_size.value
    max_unauthenticated_upload_file_size = (
        config.ipfs.max_unauthenticated_upload_file_size.value
    )

    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)
    signature_verifier = get_signature_verifier_from_request(request)

    uploaded_file = None
    metadata = None
    filename = "file"
    cid = None
    size = None

    try:
        if request.content_type != "multipart/form-data":
            raise web.HTTPBadRequest(
                reason="Expected Content-Type: multipart/form-data"
            )

        # Read the largest allowed limit here; we narrow it later once we
        # know whether metadata is present. This means unauthenticated
        # requests get a two-step check: the initial streaming cap is
        # max_upload_file_size, and a secondary check afterwards enforces
        # max_unauthenticated_upload_file_size.
        reader = await request.multipart()
        async for part in reader:
            if part is None:
                raise web.HTTPBadRequest(reason="Invalid multipart structure")
            if not isinstance(part, BodyPartReader):
                raise web.HTTPBadRequest(reason="Invalid multipart structure")

            if part.name == "file":
                filename = part.filename or "file"
                uploaded_file = MultipartUploadedFile(part, max_upload_file_size)
                await uploaded_file.read_and_validate()
            elif part.name == "metadata":
                metadata = await part.read(decode=True)

        if uploaded_file is None:
            raise web.HTTPUnprocessableEntity(
                reason="Missing 'file' in multipart form."
            )

        # Narrow the effective cap for unauthenticated requests.
        if (
            metadata is None
            and uploaded_file.size > max_unauthenticated_upload_file_size
        ):
            raise web.HTTPRequestEntityTooLarge(
                actual_size=uploaded_file.size,
                max_size=max_unauthenticated_upload_file_size,
            )

        # Parse + validate the message BEFORE pinning (fail-fast).
        message = None
        message_content = None
        sync = False
        if metadata:
            metadata_bytes = (
                metadata.file.read() if isinstance(metadata, FileField) else metadata
            )
            try:
                storage_metadata = StorageMetadata.model_validate_json(metadata_bytes)
            except ValidationError as e:
                raise web.HTTPUnprocessableEntity(
                    reason=f"Could not decode metadata: {e.json()}"
                )
            message = storage_metadata.message
            sync = storage_metadata.sync

            await _verify_message_signature(
                pending_message=message, signature_verifier=signature_verifier
            )
            if not message.item_content:
                raise web.HTTPUnprocessableEntity(reason="Store message content needed")
            try:
                message_content = CostEstimationStoreContent.model_validate_json(
                    message.item_content
                )
            except ValidationError as e:
                raise web.HTTPUnprocessableEntity(
                    reason=f"Invalid store message content: {e.json()}"
                )
            if message_content.item_type != ItemType.ipfs:
                raise web.HTTPUnprocessableEntity(
                    reason=(
                        "Expected item_type=ipfs in STORE message, "
                        f"got {message_content.item_type}"
                    )
                )

        # Pin to IPFS — side effect: file is now on the local IPFS node.
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

        # Post-pin: CID match, balance check, persist.
        # Failures from this point on must leave the pin covered by the
        # 24 h grace period so the GC doesn't strand it.
        try:
            if message_content is not None:
                message_content.estimated_size_mib = math.ceil(uploaded_file.size / MiB)
                if message_content.item_hash != cid:
                    raise web.HTTPUnprocessableEntity(
                        reason=(
                            f"File hash does not match "
                            f"({cid} != {message_content.item_hash})"
                        )
                    )
                with session_factory() as session:
                    _verify_user_balance(
                        session=session,
                        content=message_content,
                        max_unauthenticated_upload_file_size=(
                            max_unauthenticated_upload_file_size
                        ),
                    )

            with session_factory() as session:
                upsert_file(
                    session=session,
                    file_hash=cid,
                    size=size,
                    file_type=FileType.FILE,
                )
                if message_content is None:
                    add_grace_period_for_file(
                        session=session, file_hash=cid, hours=grace_period
                    )
                session.commit()
        except web.HTTPException:
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
            logger.warning(
                "Post-pin failure for %s; applied %dh grace period",
                cid,
                grace_period,
            )
            raise

        status_code = 200
        if message:
            broadcast_status = await broadcast_and_process_message(
                pending_message=message,
                sync=sync,
                request=request,
                logger=logger,
            )
            status_code = broadcast_status_to_http_status(broadcast_status)

        return web.json_response(
            data={
                "status": "success",
                "hash": cid,
                "name": filename,
                "size": size,
            },
            status=status_code,
        )

    finally:
        if uploaded_file is not None:
            await uploaded_file.cleanup()
