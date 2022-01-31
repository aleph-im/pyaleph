""" Storage module for Aleph.
Basically manages the IPFS storage.
"""

import asyncio
import json
import logging
from hashlib import sha256
from typing import Dict, IO, Optional

from aleph.services.filestore import get_value, set_value
from aleph.services.ipfs.storage import add_bytes as add_ipfs_bytes
from aleph.services.ipfs.storage import add_file as ipfs_add_file
from aleph.services.ipfs.storage import get_ipfs_content
from aleph.services.ipfs.storage import pin_add as ipfs_pin_add
from aleph.services.p2p.http import request_hash as p2p_http_request_hash
from aleph.services.p2p.singleton import get_streamer
from aleph.types import ItemType
from aleph.utils import run_in_executor, get_sha256
from aleph.web import app

LOGGER = logging.getLogger("STORAGE")


async def json_async_loads(s: str):
    """Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance
    containing a JSON document) to a Python object in an asynchronous executor."""
    return await run_in_executor(None, json.loads, s)


async def get_message_content(message: Dict):
    item_type: str = message.get("item_type", ItemType.IPFS)

    if item_type in (ItemType.IPFS, ItemType.Storage):
        return await get_json(message["item_hash"], engine=ItemType(item_type))
    elif item_type == ItemType.Inline:
        if "item_content" not in message:
            LOGGER.warning(f"No item_content in message {message.get('item_hash')}")
            return -1, 0  # never retry, bogus data
        try:
            item_content = await json_async_loads(message["item_content"])
        except (json.JSONDecodeError, KeyError):
            LOGGER.exception("Can't decode JSON")
            return -1, 0  # never retry, bogus data
        return item_content, len(message["item_content"])
    else:
        LOGGER.exception("Unknown item type: %s", item_type)
        return (
            None,
            0,
        )  # unknown, could retry later? shouldn't have arrived this far though.


async def get_hash_content(
    hash,
    engine: ItemType=ItemType.IPFS,
    timeout=2,
    tries=1,
    use_network=True,
    use_ipfs=True,
    store_value=True,
):
    # TODO: determine which storage engine to use
    ipfs_enabled = app["config"].ipfs.enabled.value
    enabled_clients = app["config"].p2p.clients.value
    # content = await loop.run_in_executor(None, get_value, hash)
    content = await get_value(hash)
    if content is None:
        if use_network:
            if "protocol" in enabled_clients:
                streamer = get_streamer()
                content = await streamer.request_hash(hash)

            if "http" in enabled_clients and content is None:
                content = await p2p_http_request_hash(hash, timeout=timeout)

        if content is not None:
            if engine == ItemType.IPFS and ipfs_enabled:
                # TODO: get a better way to compare hashes (without depending on IPFS daemon)
                try:
                    cid_version = 0
                    if len(hash) >= 58:
                        cid_version = 1

                    compared_hash = await add_ipfs_bytes(
                        content, cid_version=cid_version
                    )

                    if compared_hash != hash:
                        LOGGER.warning(f"Got a bad hash! {hash}/{compared_hash}")
                        content = -1
                except asyncio.TimeoutError:
                    LOGGER.warning(f"Can't verify hash {hash}")
                    content = None

            elif engine == ItemType.Storage:
                compared_hash = await run_in_executor(None, get_sha256, content)
                # compared_hash = sha256(content.encode('utf-8')).hexdigest()
                if compared_hash != hash:
                    LOGGER.warning(f"Got a bad hash! {hash}/{compared_hash}")
                    content = -1

        if content is None:
            if ipfs_enabled and engine == ItemType.IPFS and use_ipfs:
                content = await get_ipfs_content(hash, timeout=timeout, tries=tries)
        else:
            LOGGER.info(f"Got content from p2p {hash}")

        if content is not None and content != -1 and store_value:
            LOGGER.debug(f"Storing content for{hash}")
            await set_value(hash, content)
    else:
        LOGGER.debug(f"Using stored content for {hash}")

    return content


async def get_json(hash, engine=ItemType.IPFS, timeout=2, tries=1):
    content = await get_hash_content(hash, engine=engine, timeout=timeout, tries=tries)
    size = 0
    if content is not None and content != -1:
        size = len(content)
        try:
            content = await json_async_loads(content)
        except json.JSONDecodeError:
            LOGGER.exception("Can't decode JSON")
            content = -1  # never retry, bogus data
    return content, size


async def pin_hash(chash, timeout: int = 2, tries: int = 1):
    return await ipfs_pin_add(chash, timeout=timeout, tries=tries)


async def add_json(value, engine: ItemType = ItemType.IPFS) -> str:
    # TODO: determine which storage engine to use
    content = await run_in_executor(None, json.dumps, value)
    content = content.encode("utf-8")
    if engine == ItemType.IPFS:
        chash = await add_ipfs_bytes(content)
    elif engine == ItemType.Storage:
        if isinstance(content, str):
            content = content.encode("utf-8")
        chash = sha256(content).hexdigest()
    else:
        raise NotImplementedError("storage engine %s not supported" % engine)

    await set_value(chash, content)
    return chash


async def add_file(fileobject: IO, filename: Optional[str] = None, engine: ItemType = ItemType.IPFS):

    if engine == ItemType.IPFS:
        output = await ipfs_add_file(fileobject, filename)
        file_hash = output["Hash"]
        fileobject.seek(0)
        file_content = fileobject.read()

    elif engine == ItemType.Storage:
        file_content = fileobject.read()
        file_hash = sha256(file_content).hexdigest()

    else:
        raise ValueError(f"Unsupported item type: {engine}")

    await set_value(file_hash, file_content)
    return file_hash
