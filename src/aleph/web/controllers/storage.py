import base64
import logging

from aiohttp import web
from aleph_message.models import ItemType

from aleph.db.accessors.files import count_file_pins
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.utils import run_in_executor, item_type_from_hash

logger = logging.getLogger(__name__)


async def add_ipfs_json_controller(request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service: StorageService = request.app["storage_service"]

    data = await request.json()
    output = {
        "status": "success",
        "hash": await storage_service.add_json(data, engine=ItemType.ipfs),
    }
    return web.json_response(output)


async def add_storage_json_controller(request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service: StorageService = request.app["storage_service"]

    data = await request.json()
    output = {
        "status": "success",
        "hash": await storage_service.add_json(data, engine=ItemType.storage),
    }
    return web.json_response(output)


async def storage_add_file(request):
    storage_service: StorageService = request.app["storage_service"]

    # No need to pin it here anymore.
    # TODO: find a way to specify linked ipfs hashes in posts/aggr.
    post = await request.post()
    file_hash = await storage_service.add_file(
        post["file"].file, engine=ItemType.storage
    )

    output = {"status": "success", "hash": file_hash}
    return web.json_response(output)


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

    storage_service: StorageService = request.app["storage_service"]

    try:
        hash_content = await storage_service.get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
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

    storage_service: StorageService = request.app["storage_service"]

    try:
        content = await storage_service.get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
        )
    except AlephStorageException as e:
        raise web.HTTPNotFound(text="Not found") from e

    response = web.Response(body=content.value)
    response.enable_compression()
    return response


async def get_file_pins_count(request):
    item_hash = request.match_info.get("hash", None)
    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        count = count_file_pins(session=session, file_hash=item_hash)
    return web.json_response(data=count)
