import base64
import hashlib
import logging
import os
import tempfile
from decimal import Decimal
from typing import Optional

import aio_pika
import pydantic
from aiohttp import BodyPartReader, web
from aiohttp.web_request import FileField
from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import count_file_pins, get_file
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingInlineStoreMessage,
    PendingStoreMessage,
)
from aleph.storage import StorageService
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidSignature
from aleph.utils import get_sha256, item_type_from_hash, run_in_executor
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_mq_channel_from_request,
    get_session_factory_from_request,
    get_signature_verifier_from_request,
    get_storage_service_from_request,
)
from aleph.web.controllers.utils import (
    add_grace_period_for_file,
    broadcast_and_process_message,
    broadcast_status_to_http_status,
    mq_make_aleph_message_topic_queue,
)
from aleph_message.models import ItemType, StoreContent
from mypy.dmypy_server import MiB
from pydantic import ValidationError

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


class UploadedFile:
    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def cleanup(self):
        pass

    async def read_and_validate(self):
        pass

    @property
    def size(self) -> int:
        raise NotImplementedError

    @property
    def content(self) -> bytes:
        raise NotImplementedError

    @property
    def file(self):
        raise NotImplementedError

    @property
    def get_hash(self) -> str:
        raise NotImplementedError


class MultipartUploadedFile(UploadedFile):
    def __init__(self, file_field: BodyPartReader, max_size: int, file_hash: str = None):
        self.file_field = file_field
        self.max_size = max_size
        self.file_hash = file_hash
        try:
            self._temp_file = tempfile.NamedTemporaryFile(delete=False)
            self._file_content = bytearray()
        except Exception as e:
            web.HTTPInternalServerError(reason="Cannot create tempfile")

    def __enter__(self):
        self._temp_file.seek(0)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._temp_file.close()
            if os.path.exists(self._temp_file.name):
                os.unlink(self._temp_file.name)
        except Exception as e:
            web.HTTPInternalServerError(reason="Cannot create tempfile")

    async def read_and_validate(self):
        total_read = 0
        chunk_size = 8192
        hash_sha256 = hashlib.sha256()

        while total_read < self.max_size:
            chunk = await self.file_field.read_chunk(chunk_size)
            if not chunk:
                break
            total_read += len(chunk)
            if total_read > self.max_size:
                raise web.HTTPRequestEntityTooLarge(
                    reason="File size exceeds the maximum limit.",
                    max_size=self.max_size,
                    actual_size=total_read,
                )
            self._temp_file.write(chunk)
            self._file_content.extend(chunk)
            hash_sha256.update(chunk)
        self.file_hash = hash_sha256.hexdigest()
        self._temp_file.seek(0)

    @property
    def size(self) -> int:
        return os.path.getsize(self._temp_file.name)

    @property
    def content(self) -> bytes:
        return bytes(self._file_content)

    @property
    def file(self) -> str:
        return self._temp_file.name

    @property
    def get_hash(self) -> str:
        return self.file_hash


class RawUploadedFile(UploadedFile):
    def __init__(self, request: web.Request, max_size: int):
        self.request = request
        self.max_size = max_size
        self._temp_file = tempfile.NamedTemporaryFile(delete=False)
        self._hasher = hashlib.sha256()
        self._size = 0
        self._hash = None

    async def read_and_validate(self):
        async for chunk in self.request.content.iter_chunked(8192):
            self._temp_file.write(chunk)
            self._hasher.update(chunk)
            self._size += len(chunk)
            if self._size > self.max_size:
                raise web.HTTPRequestEntityTooLarge(
                    reason="File size exceeds the maximum limit.",
                    max_size=self.max_size,
                    actual_size=self._size,
                )
        self._temp_file.seek(0)
        self._hash = self._hasher.hexdigest()

    def __enter__(self):
        self._temp_file.seek(0)
        return self._temp_file

    def __exit__(self, exc_type, exc_value, traceback):
        self._temp_file.close()
        os.unlink(self._temp_file.name)

    @property
    def size(self) -> int:
        return self._size

    @property
    def content(self) -> bytes:
        self._temp_file.seek(0)
        return self._temp_file.read()

    @property
    def file(self):
        return self._temp_file.name

    @property
    def get_hash(self) -> str:
        if self._hash is None:
            raise ValueError("Hash has not been computed yet")
        return self._hash


async def _check_and_add_file(
        session: DbSession,
        signature_verifier: SignatureVerifier,
        storage_service: StorageService,
        message: Optional[PendingStoreMessage],
        file: UploadedFile,
        grace_period: int,
) -> str:
    file_hash = file.get_hash
    # Perform authentication and balance checks
    if message:
        await _verify_message_signature(
            pending_message=message, signature_verifier=signature_verifier
        )
        try:
            message_content = StoreContent.parse_raw(message.item_content)
            if message_content.item_hash != file_hash:
                raise web.HTTPUnprocessableEntity(
                    reason=f"File hash does not match ({file_hash} != {message_content.item_hash})"
                )
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

    file_content = file.content
    file_bytes = (
        file_content.encode("utf-8") if isinstance(file_content, str) else file_content
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
    metadata = None
    uploaded_file: Optional[UploadedFile] = None

    if request.content_type == "multipart/form-data":
        reader = await request.multipart()
        async for part in reader:
            if part.name == 'file':
                uploaded_file = MultipartUploadedFile(part, MAX_FILE_SIZE)
                await uploaded_file.read_and_validate()
            elif part.name == 'metadata':
                metadata = await part.read(decode=True)
    else:
        uploaded_file = RawUploadedFile(request=request, max_size=MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE)
        await uploaded_file.read_and_validate()

    max_upload_size = (
        MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE if not metadata else MAX_FILE_SIZE
    )

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
    else:
        message = None
        sync = False

    if uploaded_file.size > max_upload_size:
        raise web.HTTPRequestEntityTooLarge(
            actual_size=uploaded_file.size, max_size=max_upload_size
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
