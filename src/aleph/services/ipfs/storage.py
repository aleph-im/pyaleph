import asyncio
import concurrent
import json
import logging
from typing import Optional

import aiohttp
import aioipfs

from .common import get_ipfs_api, get_base_url
from ...utils import run_in_executor

LOGGER = logging.getLogger("IPFS.STORAGE")

MAX_LEN = 1024 * 1024 * 100


async def get_ipfs_content(hash: str, timeout: int = 1, tries: int = 1) -> Optional[bytes]:
    try_count = 0
    result = None
    while (result is None) and (try_count < tries):
        try_count += 1
        try:
            api = await get_ipfs_api(timeout=5)
            result = await asyncio.wait_for(api.cat(hash, length=MAX_LEN), timeout=timeout)
            if len(result) == MAX_LEN:
                result = None
                break
        except aioipfs.APIError:
            result = None
            await asyncio.sleep(0.5)
            continue
        except (asyncio.TimeoutError):
            result = None
            await asyncio.sleep(0.5)
        except (
            concurrent.futures.CancelledError,
            aiohttp.client_exceptions.ClientConnectorError,
        ):
            try_count -= 1  # do not count as a try.
            await asyncio.sleep(0.1)

    if isinstance(result, str):
        result = result.encode("utf-8")

    return result


async def get_json(hash, timeout=1, tries=1):
    result = await get_ipfs_content(hash, timeout=timeout, tries=tries)
    if result is not None and result != -1:
        try:
            result = await run_in_executor(None, json.loads, result)
        except json.decoder.JSONDecodeError:
            # try:
            #     import json as njson
            #     result = await loop.run_in_executor(None, njson.loads, result)
            # except (json.JSONDecodeError, KeyError):
            LOGGER.exception("Can't decode JSON")
            result = -1  # never retry, bogus data
    return result


async def add_json(value: bytes) -> str:
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=5)
    # try:
    result = await api.add_json(value)
    # finally:
    #     await api.close()

    return result["Hash"]


async def add_bytes(value: bytes, cid_version: int = 0) -> str:
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=5)
    # try:
    result = await api.add_bytes(value, cid_version=cid_version)
    # finally:
    #     await api.close()

    return result["Hash"]


async def pin_add(hash: str, timeout: int = 2, tries: int = 1):
    # loop = asyncio.get_event_loop()
    try_count = 0
    result = None
    while (result is None) and (try_count < tries):
        try_count += 1
        api = await get_ipfs_api(timeout=timeout)
        try:
            result = None
            async for ret in api.pin.add(hash):
                result = ret
        except (asyncio.TimeoutError, json.JSONDecodeError):
            result = None
        except concurrent.futures.CancelledError:
            try_count -= 1  # do not count as a try.
            await asyncio.sleep(0.1)
        # finally:
        #     await api.close()

    return result


async def add_file(fileobject, filename):
    async with aiohttp.ClientSession() as session:
        from aleph.web import app

        url = "%s/api/v0/add" % (await get_base_url(app["config"]))
        data = aiohttp.FormData()
        data.add_field("path", fileobject, filename=filename)

        resp = await session.post(url, data=data)
        return await resp.json()
