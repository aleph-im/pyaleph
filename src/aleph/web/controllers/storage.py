import ast
import base64
import datetime as dt
import functools
import logging
from hashlib import sha256
from typing import Union, Tuple
from decimal import Decimal
import aio_pika
import pydantic
from aiohttp.web_response import Response
from eth_account import Account
from eth_account.messages import encode_defunct
from mypy.dmypy_server import MiB
from sqlalchemy.orm import Session

from aleph.chains.chain_service import ChainService
from aleph.chains.common import get_verification_buffer

from aiohttp import web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType
from multidict import MultiDictProxy
from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import count_file_pins, get_file
from aleph.db.models import PendingMessageDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.toolkit import json
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import (
    MessageProcessingStatus,
    InvalidSignature,
    InvalidMessageException,
)
from aleph.utils import run_in_executor, item_type_from_hash
from aleph.web.controllers.app_state_getters import (
    get_session_factory_from_request,
    get_storage_service_from_request,
    get_config_from_request,
    get_mq_channel_from_request,
    get_chain_service_from_request,
)

from aleph.web.controllers.utils import (
    multidict_proxy_to_io,
    mq_make_aleph_message_topic_queue,
    processing_status_to_http_status,
    mq_read_one_message,
    validate_message_dict,
)
from aleph.schemas.pending_messages import BasePendingMessage, PendingStoreMessage

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 100 * 1024 * 1024


async def add_ipfs_json_controller(request: web.Request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)

    data = await request.json()
    with session_factory() as session:
        output = {
            "status": "success",
            "hash": await storage_service.add_json(
                session=session, value=data, engine=ItemType.ipfs
            ),
        }
        session.commit()

    return web.json_response(output)


async def add_storage_json_controller(request: web.Request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)

    data = await request.json()
    with session_factory() as session:
        output = {
            "status": "success",
            "hash": await storage_service.add_json(
                session=session, value=data, engine=ItemType.storage
            ),
        }
        session.commit()

    return web.json_response(output)


async def _verify_message_signature(
    pending_message: BasePendingMessage, chain_service: ChainService
) -> None:
    try:
        await chain_service.verify_signature(pending_message)
    except InvalidSignature:
        raise web.HTTPForbidden()


async def _verify_user_balance(
    pending_message_db: PendingMessageDb, session: DbSession, size: int
) -> None:
    current_balance = get_total_balance(
        session=session, address=pending_message_db.sender
    ) or Decimal(0)
    required_balance = (size / MiB) / 3
    current_cost_for_user = get_total_cost_for_address(
        session=session, address=pending_message_db.sender
    )
    if current_balance < (Decimal(required_balance) + current_cost_for_user):
        raise web.HTTPPaymentRequired()


async def _verify_user_file(message: PendingStoreMessage, size: int, file_io) -> None:
    content = file_io.read(size)
    item_content = {}
    if message.item_content:
        item_content = json.loads(message.item_content)
    actual_item_hash = sha256(content).hexdigest()
    client_item_hash = item_content["item_hash"]
    if len(content) > (1000 * MiB):
        raise web.HTTPRequestEntityTooLarge(
            actual_size=len(content), max_size=(1000 * MiB)
        )
    elif actual_item_hash != client_item_hash:
        raise web.HTTPUnprocessableEntity()


class StorageMetadata(pydantic.BaseModel):
    message: PendingStoreMessage
    file_size: int
    sync: bool


async def storage_add_file_with_message(
    request: web.Request, session: DbSession, chain_service, storage_metadata, file_io
):
    config = get_config_from_request(request)
    mq_queue = None

    pending_store_message = PendingStoreMessage.parse_obj(storage_metadata.message)
    pending_message_db = PendingMessageDb.from_obj(
        obj=pending_store_message, reception_time=dt.datetime.now(), fetched=True
    )
    await _verify_message_signature(
        pending_message=storage_metadata.message, chain_service=chain_service
    )
    await _verify_user_balance(
        session=session,
        pending_message_db=pending_message_db,
        size=storage_metadata.file_size,
    )
    await _verify_user_file(
        message=storage_metadata.message,
        size=storage_metadata.file_size,
        file_io=file_io,
    )

    if storage_metadata.sync:
        mq_channel = await get_mq_channel_from_request(request, logger=logger)
        mq_queue = await mq_make_aleph_message_topic_queue(
            channel=mq_channel,
            config=config,
            routing_key=f"*.{pending_message_db.item_hash}",
        )

    session.add(pending_message_db)
    session.commit()
    if storage_metadata.sync and mq_queue:
        mq_message = await mq_read_one_message(mq_queue, 30)

        if mq_message is None:
            raise web.HTTPAccepted()
        if mq_message.routing_key is not None:
            status_str, _item_hash = mq_message.routing_key.split(".")
            processing_status = MessageProcessingStatus(status_str)
            status_code = processing_status_to_http_status(processing_status)
            return web.json_response(status=status_code, text=_item_hash)


async def storage_add_file(request: web.Request):
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    # TODO : Add chainservice to ccn_api_client to be able to call get_chainservice_from_request
    chain_service: ChainService = ChainService(
        session_factory=session_factory, storage_service=storage_service
    )
    post = await request.post()
    file_io = multidict_proxy_to_io(post)
    post = await request.post()
    metadata = post.get("metadata", b"")
    storage_metadata = None
    try:
        if isinstance(metadata, FileField):
            metadata_content = metadata.file.read()
            storage_metadata = StorageMetadata.parse_raw(metadata_content)
        else:
            storage_metadata = StorageMetadata.parse_raw(metadata)
    except Exception as e:
        if metadata:
            raise web.HTTPUnprocessableEntity()
    if metadata is None:
        if len(file_io.read()) > (25 * MiB):
            raise web.HTTPUnauthorized()
    file_io.seek(0)

    with session_factory() as session:
        file_hash = await storage_service.add_file(
            session=session, fileobject=file_io, engine=ItemType.storage
        )
        if storage_metadata:
            await storage_add_file_with_message(
                request, session, chain_service, storage_metadata, file_io
            )
        session.commit()
    output = {"status": "success", "hash": file_hash}
    return web.json_response(output)


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
