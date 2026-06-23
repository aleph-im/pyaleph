import asyncio
import base64
import logging
import math

from aiohttp import BodyPartReader, web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType
from pydantic import ValidationError

from aleph.db.accessors.files import upsert_file
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.services.ipfs.service import InvalidIpnsRecordError
from aleph.toolkit.car import InvalidCarFile, read_carv1_root
from aleph.toolkit.constants import MiB
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_session_factory_from_request,
    get_signature_verifier_from_request,
)
from aleph.web.controllers.storage import (
    MultipartUploadedCar,
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


async def _verify_ipns_record_and_get_cid(
    request: web.Request, message_content: CostEstimationStoreContent
) -> str:
    """Decode and verify the IPNS record embedded in *message_content*.

    Returns the CID that the record points to (``value_cid``).  Raises
    :class:`aiohttp.web.HTTPUnprocessableEntity` if the record is absent or
    invalid.
    """
    if message_content.ipns_record is None:
        raise web.HTTPUnprocessableEntity(
            reason="IPNS store messages attached to uploads must include ipns_record"
        )
    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")
    record = base64.b64decode(message_content.ipns_record)
    try:
        record_info = await ipfs_service.verify_ipns_record(
            record, message_content.item_hash
        )
    except InvalidIpnsRecordError as e:
        raise web.HTTPUnprocessableEntity(reason=f"Invalid IPNS record: {e}")
    return record_info.value_cid


async def _check_ipns_upload(
    request: web.Request, message_content: CostEstimationStoreContent, cid: str
) -> None:
    """For IPNS STORE metadata the uploaded CID must match the value inside
    the signed record, not the message item_hash (which is the IPNS name).
    """
    value_cid = await _verify_ipns_record_and_get_cid(request, message_content)
    if value_cid != cid:
        raise web.HTTPUnprocessableEntity(
            reason=(
                f"IPNS record value does not match the uploaded file "
                f"({cid} != {value_cid})"
            )
        )


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
                  (item_type=ipfs or ipns). When present, the CID computed after
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

        # Validate the signed message and balance BEFORE pinning so neither
        # a bad signature nor an underfunded account leaves an orphan pin on
        # the IPFS daemon. By this point the file is already buffered to a
        # temp file (multipart parts are consumed in arrival order); we gate
        # the pin step, not the multipart read.
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
            if message_content.item_type not in (ItemType.ipfs, ItemType.ipns):
                raise web.HTTPUnprocessableEntity(
                    reason=(
                        "Expected item_type=ipfs or item_type=ipns in STORE message, "
                        f"got {message_content.item_type}"
                    )
                )

            message_content.estimated_size_mib = math.ceil(uploaded_file.size / MiB)
            with session_factory() as session:
                _verify_user_balance(session=session, content=message_content)

        # Pin to IPFS, side effect: file is now on the local IPFS node.
        temp_file = await uploaded_file.open_temp_file()
        file_content = await temp_file.read()
        if isinstance(file_content, str):
            file_content = file_content.encode("utf-8")

        cid = await ipfs_service.add_bytes(file_content)

        # Post-pin: stat, CID match, persist.
        # Failures from this point on must leave the pin covered by the
        # 24 h grace period so the GC doesn't strand it.
        try:
            try:
                stats = await asyncio.wait_for(
                    ipfs_service.storage_client.files.stat(f"/ipfs/{cid}"),
                    config.ipfs.stat_timeout.value,
                )
            except TimeoutError:
                raise web.HTTPGatewayTimeout(reason="Timed out waiting for IPFS stat")
            size = stats["Size"]

            if message_content is not None:
                if message_content.item_type == ItemType.ipns:
                    await _check_ipns_upload(request, message_content, cid)
                elif message_content.item_hash != cid:
                    raise web.HTTPUnprocessableEntity(
                        reason=(
                            f"File hash does not match "
                            f"({cid} != {message_content.item_hash})"
                        )
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
        except Exception:
            # Bare `Exception` is intentional: any post-pin failure must
            # apply the grace period, including non-HTTP errors like DB
            # outages or library timeouts. Without this catch, the pin
            # would be left on the IPFS daemon with no record in our DB,
            # which the GC has no way to reap. We re-raise after applying.
            # size may be unset here (if stat itself failed); fall back to
            # the size we already know from multipart read.
            fallback_size = size if size is not None else uploaded_file.size
            try:
                with session_factory() as session:
                    upsert_file(
                        session=session,
                        file_hash=cid,
                        size=fallback_size,
                        file_type=FileType.FILE,
                    )
                    add_grace_period_for_file(
                        session=session, file_hash=cid, hours=grace_period
                    )
                    session.commit()
            except Exception:
                logger.exception("Failed to apply grace period for orphan pin %s", cid)
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


async def ipfs_add_car(request: web.Request):
    """Upload a CARv1 directory archive to IPFS, authenticated.

    ---
    summary: Add a CAR (directory) to IPFS
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
              - metadata
            properties:
              file:
                type: string
                format: binary
                description: CARv1 bytes, single root.
              metadata:
                type: string
                description: >
                  JSON {"message": <PendingInlineStoreMessage>, "sync": bool}.
                  message.content.item_type must be "ipfs" or "ipns"; item_hash must
                  equal the CAR's single root CID.
    responses:
      '200':
        description: Upload + STORE broadcast succeeded.
      '402':
        description: Insufficient balance for the STORE message.
      '403':
        description: IPFS disabled, or signature invalid.
      '413':
        description: CAR exceeds ipfs.max_upload_car_size.
      '422':
        description: Invalid multipart, metadata, CAR header, or CID mismatch.
      '502':
        description: kubo /dag/import failed.
      '504':
        description: Timed out waiting for IPFS stat.
    """
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value
    max_upload_car_size = config.ipfs.max_upload_car_size.value

    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)
    signature_verifier = get_signature_verifier_from_request(request)

    uploaded_file: MultipartUploadedCar | None = None
    metadata = None
    cid: str | None = None
    size: int | None = None

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
                uploaded_file = MultipartUploadedCar(part, max_upload_car_size)
                await uploaded_file.read_and_validate()
            elif part.name == "metadata":
                metadata = await part.read(decode=True)

        if uploaded_file is None:
            raise web.HTTPUnprocessableEntity(
                reason="Missing 'file' in multipart form."
            )
        if metadata is None:
            raise web.HTTPUnprocessableEntity(
                reason="metadata is required for CAR upload"
            )

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
        if message_content.item_type not in (ItemType.ipfs, ItemType.ipns):
            raise web.HTTPUnprocessableEntity(
                reason=(
                    "Expected item_type=ipfs or item_type=ipns in STORE message, "
                    f"got {message_content.item_type}"
                )
            )

        # Parse the CAR header and verify the declared root matches the
        # metadata BEFORE any kubo contact. Failures here have zero IPFS
        # side effects.
        car_path = uploaded_file.get_temp_file_path()
        try:
            car_root = read_carv1_root(car_path)
        except InvalidCarFile as e:
            raise web.HTTPUnprocessableEntity(reason=f"Invalid CAR file: {e}")
        if message_content.item_type == ItemType.ipns:
            # For IPNS, the expected root is the CID inside the signed record,
            # not the item_hash (which is the IPNS name).
            expected_root = await _verify_ipns_record_and_get_cid(
                request, message_content
            )
            if car_root != expected_root:
                raise web.HTTPUnprocessableEntity(
                    reason=(
                        f"Root CID does not match " f"({car_root} != {expected_root})"
                    )
                )
        else:
            expected_root = message_content.item_hash
            if car_root != expected_root:
                raise web.HTTPUnprocessableEntity(
                    reason=(
                        f"Root CID does not match " f"({car_root} != {expected_root})"
                    )
                )

        # Balance check BEFORE dag_import so an underfunded request leaves no
        # pin on kubo. CAR file size is a conservative overestimate of the
        # directory's CumulativeSize (CAR adds block framing on top of the
        # same payload bytes), which is the right direction for a pre-pin
        # gate.
        message_content.estimated_size_mib = math.ceil(uploaded_file.size / MiB)
        with session_factory() as session:
            _verify_user_balance(session=session, content=message_content)

        # Import the CAR into kubo. From this point on, the root is pinned
        # and any failure must apply the grace-period cleanup.
        try:
            imported_roots = await ipfs_service.dag_import(car_path, pin_roots=True)
        except Exception as e:
            raise web.HTTPBadGateway(reason=f"Failed to import CAR into IPFS: {e}")
        if len(imported_roots) != 1 or imported_roots[0] != expected_root:
            kubo_root = imported_roots[0] if imported_roots else "<none>"
            # Defensive branch: kubo verified each block during import and
            # reported a different root than the CAR header declared. The
            # pre-import header check (line ~410) already matched expected_root,
            # so reaching here means kubo's view of the DAG disagrees with the
            # CAR header, indicating a malformed CAR. Any blocks kubo holds for this DAG
            # are unpinned (pin-roots pinned only what kubo computed as roots
            # from valid blocks; mismatched header roots are not pinned) and
            # will be reaped by kubo's periodic GC. We do not write a grace-
            # period row because there is no canonical CID to track.
            raise web.HTTPUnprocessableEntity(
                reason=(
                    f"Imported root does not match expected "
                    f"({kubo_root} != {expected_root}); CAR header "
                    f"declared a root that does not correspond to the "
                    f"imported DAG"
                )
            )
        cid = expected_root

        try:
            try:
                stats = await asyncio.wait_for(
                    ipfs_service.storage_client.files.stat(f"/ipfs/{cid}"),
                    config.ipfs.stat_timeout.value,
                )
            except TimeoutError:
                raise web.HTTPGatewayTimeout(reason="Timed out waiting for IPFS stat")
            # Directories: `Size` is 0, use `CumulativeSize`.
            size = stats.get("CumulativeSize", stats.get("Size", 0))

            with session_factory() as session:
                upsert_file(
                    session=session,
                    file_hash=cid,
                    size=size,
                    file_type=FileType.DIRECTORY,
                )
                session.commit()
        except Exception:
            if cid is None:
                raise RuntimeError("post-import failure path requires cid to be set")
            fallback_size = size if size is not None else uploaded_file.size
            try:
                with session_factory() as session:
                    upsert_file(
                        session=session,
                        file_hash=cid,
                        size=fallback_size,
                        file_type=FileType.DIRECTORY,
                    )
                    add_grace_period_for_file(
                        session=session, file_hash=cid, hours=grace_period
                    )
                    session.commit()
            except Exception:
                logger.exception("Failed to apply grace period for orphan pin %s", cid)
            logger.warning(
                "Post-import failure for %s; applied %dh grace period",
                cid,
                grace_period,
            )
            raise

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
                "size": size,
            },
            status=status_code,
        )

    finally:
        if uploaded_file is not None:
            await uploaded_file.cleanup()
