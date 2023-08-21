import asyncio
import base64
import datetime as dt
import functools
import logging
from hashlib import sha256
from io import StringIO
from typing import Union, Tuple, Dict, Optional

import aio_pika
from eth_account import Account
from eth_account.messages import encode_defunct

from aleph.chains.common import get_verification_buffer
from aleph.jobs.process_pending_messages import PendingMessageProcessor

from aiohttp import web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType
from multidict import MultiDictProxy

from aleph.chains.chain_service import ChainService, LOGGER
from aleph.chains.nuls import NulsConnector
from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.files import count_file_pins, get_file
from aleph.db.accessors.messages import get_message_status, message_exists
from aleph.db.connection import make_session_factory
from aleph.db.models import PendingMessageDb
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.services.p2p import init_p2p_client
from aleph.services.storage.engine import StorageEngine
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit import json
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory, DbSession
from aleph.utils import run_in_executor, item_type_from_hash
from aleph.web.controllers.app_state_getters import (
    get_session_factory_from_request,
    get_storage_service_from_request, get_mq_channel_from_request, get_config_from_request, get_mq_conn_from_request,
)
from aleph.web.controllers.utils import multidict_proxy_to_io
from aleph.schemas.pending_messages import BasePendingMessage

logger = logging.getLogger(__name__)
from aleph.schemas.pending_messages import parse_message

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


async def verify_signature(message: BasePendingMessage) -> bool:
    """Verifies a signature of a message, return True if verified, false if not"""
    verification = get_verification_buffer(message)

    message_hash = await run_in_executor(
        None, functools.partial(encode_defunct, text=verification.decode("utf-8"))
    )

    verified = False
    try:
        # we assume the signature is a valid string
        address = await run_in_executor(
            None,
            functools.partial(
                Account.recover_message, message_hash, signature=message.signature
            ),
        )
        if address == message.sender:
            verified = True
        else:
            return False

    except Exception as e:
        verified = False
    return verified


async def get_message_content(post_data: MultiDictProxy[Union[str, bytes, FileField]]) -> Tuple[dict, int]:
    message_bytearray = post_data.get("message", b"")
    value = post_data.get("size") or 0
    if not message_bytearray:
        return {}, int(value)  # Empty dictionary if no message content

    message_string = message_bytearray.decode("utf-8")
    message_dict = json.loads(message_string)
    message_dict["time"] = float(message_dict["time"])

    return message_dict, int(value)


async def init_mq_con(config):
    return await aio_pika.connect_robust(
        host=config.p2p.mq_host.value, port=config.rabbitmq.port.value, login=config.rabbitmq.username.value,
        password=config.rabbitmq.password.value
    )


async def verify_and_handle_request(pending_message_db, file_io, message, size):
    content = file_io.read(size)
    item_content = json.loads(message["item_content"])
    actual_item_hash = sha256(content).hexdigest()
    c_item_hash = item_content["item_hash"]

    is_signature = await verify_signature(message=pending_message_db)

    if not is_signature:
        output = {"status": "Forbidden"}
        return web.json_response(output, status=403)
    elif actual_item_hash != c_item_hash:
        output = {"status": "Unprocessable Content"}
        return web.json_response(output, status=422)
    elif len(content) > 25_000 and not message:
        output = {"status": "Unauthorized"}
        return web.json_response(output, status=401)
    else:
        return None


async def storage_add_file_with_message(request: web.Request):
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    config = get_config_from_request(request)
    mq_con = init_mq_con(config)

    post = await request.post()
    file_io = multidict_proxy_to_io(post)
    message, size = await get_message_content(post)
    pending_message_db = PendingMessageDb.from_message_dict(message_dict=message, reception_time=dt.datetime.now(),
                                                            fetched=True)
    is_valid_message = await verify_and_handle_request(pending_message_db, file_io, message, size)
    if is_valid_message is not None:
        return is_valid_message

    with session_factory() as session:
        file_hash = await storage_service.add_file(
            session=session, fileobject=file_io, engine=ItemType.storage
        )
        session.add(pending_message_db)
        session.commit()
    output = {"status": "success", "hash": file_hash}
    return web.json_response(output)


async def storage_add_file(request: web.Request):
    post = await request.post()
    if post.get("message", b"") is not None and post.get("size") is not None:
        return await storage_add_file_with_message(request)

    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    file_io = multidict_proxy_to_io(post)
    with session_factory() as session:
        file_hash = await storage_service.add_file(
            session=session, fileobject=file_io, engine=ItemType.storage
        )
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
