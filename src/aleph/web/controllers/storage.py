import base64
import logging
from decimal import Decimal
from typing import Union, Optional, Protocol

import aio_pika
import pydantic
from aiohttp import web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType, StoreContent
from mypy.dmypy_server import MiB
from pydantic import ValidationError

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import (
    count_file_pins,
    get_file,
)
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingStoreMessage,
    PendingInlineStoreMessage,
)
from aleph.storage import StorageService
from aleph.types.db_session import DbSession
from aleph.types.message_status import (
    InvalidSignature,
)
from aleph.utils import run_in_executor, item_type_from_hash, get_sha256
from aleph.web.controllers.app_state_getters import (
    get_session_factory_from_request,
    get_storage_service_from_request,
    get_config_from_request,
    get_mq_channel_from_request,
    get_signature_verifier_from_request,
)
from aleph.web.controllers.utils import (
    mq_make_aleph_message_topic_queue,
    broadcast_and_process_message,
    broadcast_status_to_http_status,
    add_grace_period_for_file,
)

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 100 * MiB
MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE = 25 * MiB
MAX_UPLOAD_FILE_SIZE = 1000 * MiB


async def add_ipfs_json_controller(request: web.Request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

    data = await request.json()
    with session_factory() as session:
        output = {
            "status": "success",
            "hash": await storage_service.add_json(
                session=session, value=data, engine=ItemType.ipfs
            ),
        }
        add_grace_period_for_file(
            session=session, file_hash=output["hash"], hours=grace_period
        )
        session.commit()

    return web.json_response(output)


async def add_storage_json_controller(request: web.Request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

    data = await request.json()
    with session_factory() as session:
        output = {
            "status": "success",
            "hash": await storage_service.add_json(
                session=session, value=data, engine=ItemType.storage
            ),
        }
        add_grace_period_for_file(
            session=session, file_hash=output["hash"], hours=grace_period
        )
        session.commit()

    return web.json_response(output)


async def _verify_message_signature(
    pending_message: BasePendingMessage, signature_verifier: SignatureVerifier
) -> None:
    try:
        await signature_verifier.verify_signature(pending_message)
    except InvalidSignature:
        raise web.HTTPForbidden()


async def _verify_user_balance(session: DbSession, address: str, size: int) -> None:
    current_balance = get_total_balance(session=session, address=address) or Decimal(0)
    required_balance = (size / MiB) / 3
    current_cost_for_user = get_total_cost_for_address(session=session, address=address)
    if size > 25 * MiB:
        if current_balance < (Decimal(required_balance) + current_cost_for_user):
            raise web.HTTPPaymentRequired()


class StorageMetadata(pydantic.BaseModel):
    message: PendingInlineStoreMessage
    sync: bool


class UploadedFile(Protocol):
    @property
    def size(self) -> int:
        ...

    @property
    def content(self) -> Union[str, bytes]:
        ...


class MultipartUploadedFile:
    _content: Optional[bytes]
    size: int

    def __init__(self, file_field: FileField, size: int):
        self.file_field = file_field
        self.size = size
        self._content = None

    @property
    def content(self) -> bytes:
        # Only read the stream once
        if self._content is None:
            self._content = self.file_field.file.read(self.size)

        return self._content


class RawUploadedFile:
    def __init__(self, content: Union[bytes, str]):
        self.content = content

    @property
    def size(self) -> int:
        return len(self.content)


async def _check_and_add_file(
    session: DbSession,
    signature_verifier: SignatureVerifier,
    storage_service: StorageService,
    message: Optional[PendingStoreMessage],
    file: UploadedFile,
    grace_period: int,
) -> str:
    # Perform authentication and balance checks
    if message:
        await _verify_message_signature(
            pending_message=message, signature_verifier=signature_verifier
        )
        try:
            message_content = StoreContent.parse_raw(message.item_content)
        except ValidationError as e:
            raise web.HTTPUnprocessableEntity(
                reason=f"Invalid store message content: {e.json()}"
            )

        await _verify_user_balance(
            session=session,
            address=message_content.address,
            size=file.size,
        )

    else:
        message_content = None

    # TODO: this can still reach 1 GiB in memory. We should look into streaming.
    file_content = file.content
    file_bytes = (
        file_content.encode("utf-8") if isinstance(file_content, str) else file_content
    )
    file_hash = get_sha256(file_content)

    if message_content:
        if message_content.item_hash != file_hash:
            raise web.HTTPUnprocessableEntity(
                reason=f"File hash does not match ({file_hash} != {message_content.item_hash})"
            )

    await storage_service.add_file_content_to_local_storage(
        session=session,
        file_content=file_bytes,
        file_hash=file_hash,
    )

    # For files uploaded without authenticated upload, add a grace period of 1 day.
    if not message_content:
        add_grace_period_for_file(
            session=session, file_hash=file_hash, hours=grace_period
        )

    return file_hash


async def _make_mq_queue(
    request: web.Request,
    sync: bool,
    routing_key: Optional[str] = None,
) -> Optional[aio_pika.abc.AbstractQueue]:
    if not sync:
        return None

    mq_channel = await get_mq_channel_from_request(request, logger)
    config = get_config_from_request(request)
    return await mq_make_aleph_message_topic_queue(
        channel=mq_channel, config=config, routing_key=routing_key
    )


async def storage_add_file(request: web.Request):
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    signature_verifier = get_signature_verifier_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

    post = await request.post()
    headers = request.headers
    try:
        file_field = post["file"]
    except KeyError:
        raise web.HTTPUnprocessableEntity(reason="Missing 'file' in multipart form.")

    if isinstance(file_field, FileField):
        try:
            content_length = int(headers.get("Content-Length", file_field.headers["Content-Length"]))
            uploaded_file: UploadedFile = MultipartUploadedFile(file_field, content_length)
        except (KeyError, ValueError):
            raise web.HTTPUnprocessableEntity(reason="Invalid/missing Content-Length header.")
    else:
        uploaded_file = RawUploadedFile(file_field)

    metadata = post.get("metadata")

    status_code = 200

    if metadata:
        metadata_bytes = (
            metadata.file.read() if isinstance(metadata, FileField) else metadata
        )
        try:
            storage_metadata = StorageMetadata.parse_raw(metadata_bytes)
        except ValidationError as e:
            raise web.HTTPUnprocessableEntity(
                reason=f"Could not decode metadata: {e.json()}"
            )

        message = storage_metadata.message
        sync = storage_metadata.sync
        max_upload_size = MAX_UPLOAD_FILE_SIZE

    else:
        # User did not provide a message in the `metadata` field
        message = None
        sync = False
        max_upload_size = MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE

    if uploaded_file.size > max_upload_size:
        raise web.HTTPRequestEntityTooLarge(
            actual_size=uploaded_file.size, max_size=MAX_UPLOAD_FILE_SIZE
        )

    with session_factory() as session:
        file_hash = await _check_and_add_file(
            session=session,
            signature_verifier=signature_verifier,
            storage_service=storage_service,
            message=message,
            file=uploaded_file,
            grace_period=grace_period,
        )
        session.commit()

    if message:
        broadcast_status = await broadcast_and_process_message(
            pending_message=message, sync=sync, request=request, logger=logger
        )
        status_code = broadcast_status_to_http_status(broadcast_status)

    output = {"status": "success", "hash": file_hash}
    return web.json_response(data=output, status=status_code)


def assert_file_is_downloadable(session: DbSession, file_hash: str) -> None:
    """
    Check if the file is on the aleph.im network and can be downloaded from the API.
    This filters out requests for files outside the network / nonexistent files.
    """
    file_metadata = get_file(session=session, file_hash=file_hash)
    if not file_metadata:
        raise web.HTTPNotFound(text="Not found")

    if file_metadata.size > MAX_FILE_SIZE:
        raise web.HTTPRequestEntityTooLarge(
            max_size=MAX_FILE_SIZE, actual_size=file_metadata.size
        )


def prepare_content(content):
    return base64.encodebytes(content).decode("utf-8")


async def get_hash(request):
    item_hash = request.match_info.get("hash", None)
    if item_hash is None:
        return web.HTTPBadRequest(text="No hash provided")
    try:
        engine = item_type_from_hash(item_hash)
    except UnknownHashError as e:
        logger.warning(e.args[0])
        return web.HTTPBadRequest(text="Invalid hash provided")

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        assert_file_is_downloadable(session=session, file_hash=item_hash)

    storage_service = get_storage_service_from_request(request)

    try:
        hash_content = await storage_service.get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
            timeout=30,
        )
    except AlephStorageException:
        return web.HTTPNotFound(text=f"No file found for hash {item_hash}")

    content = await run_in_executor(None, prepare_content, hash_content.value)
    result = {
        "status": "success",
        "hash": item_hash,
        "engine": engine,
        "content": content,
    }

    response = await run_in_executor(None, web.json_response, result)
    response.enable_compression()
    return response


async def get_raw_hash(request):
    item_hash = request.match_info.get("hash", None)

    if item_hash is None:
        raise web.HTTPBadRequest(text="No hash provided")

    try:
        engine = item_type_from_hash(item_hash)
    except UnknownHashError:
        raise web.HTTPBadRequest(text="Invalid hash")

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        assert_file_is_downloadable(session=session, file_hash=item_hash)

    storage_service = get_storage_service_from_request(request)

    try:
        content = await storage_service.get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
            timeout=30,
        )
    except AlephStorageException as e:
        raise web.HTTPNotFound(text="Not found") from e

    response = web.Response(body=content.value)
    response.enable_compression()
    return response


async def get_file_pins_count(request: web.Request) -> web.Response:
    item_hash = request.match_info.get("hash", None)

    if item_hash is None:
        raise web.HTTPBadRequest(text="No hash provided")

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        count = count_file_pins(session=session, file_hash=item_hash)
    return web.json_response(data=count)
