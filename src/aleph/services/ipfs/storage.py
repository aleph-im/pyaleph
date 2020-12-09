import aioipfs
import aiohttp
import asyncio
import json
import aiohttp
import concurrent
import logging

from .common import get_ipfs_gateway_url, get_ipfs_api, get_base_url
LOGGER = logging.getLogger("IPFS.STORAGE")

MAX_LEN = 1024*1024*20

async def get_ipfs_content(hash, timeout=1, tries=1):
    try_count = 0
    result = None
    while (result is None) and (try_count < tries):
        try_count += 1
        try:
            api = await get_ipfs_api(timeout=5)
            result = await asyncio.wait_for(api.cat(hash, length=MAX_LEN), 5)
            if len(result) == MAX_LEN:
            	result = None
            	break
        except aioipfs.APIError:
            result = None
            await asyncio.sleep(.5)
            continue
        except (asyncio.TimeoutError):
            result = None
            await asyncio.sleep(.5)
        except (concurrent.futures.CancelledError,
                aiohttp.client_exceptions.ClientConnectorError):
            try_count -= 1  # do not count as a try.
            await asyncio.sleep(.1)
            
    if isinstance(result, str):
        result = result.encode('utf-8')

    return result

async def get_json(hash, timeout=1, tries=1):
    result = await get_ipfs_content(hash, timeout=timeout, tries=tries)
    loop = asyncio.get_event_loop()
    if result is not None and result != -1:
        try:
            result = await loop.run_in_executor(None, json.loads, result)
        except json.decoder.JSONDecodeError:
            # try:
            #     import json as njson
            #     result = await loop.run_in_executor(None, njson.loads, result)
            # except (json.JSONDecodeError, KeyError): 
            LOGGER.exception("Can't decode JSON")
            result = -1  # never retry, bogus data
    return result

async def add_json(value):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=5)
    # try:
    result = await api.add_json(value)
    # finally:
    #     await api.close()

    return result['Hash']


async def add_bytes(value):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=5)
    # try:
    result = await api.add_bytes(value)
    # finally:
    #     await api.close()

    return result['Hash']


async def pin_add(hash, timeout=2, tries=1):
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
            await asyncio.sleep(.1)
        # finally:
        #     await api.close()

    return result


async def add_file(fileobject, filename):
    async with aiohttp.ClientSession() as session:
        from aleph.web import app
        url = "%s/api/v0/add" % (await get_base_url(app['config']))
        data = aiohttp.FormData()
        data.add_field('path',
                       fileobject,
                       filename=filename)

        resp = await session.post(url, data=data)
        return await resp.json()