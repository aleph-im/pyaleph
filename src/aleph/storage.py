""" Storage module for Aleph.
Basically manages the IPFS storage.
"""

import aioipfs
import aiohttp
import asyncio
import orjson as json
import aiohttp
import concurrent
import logging

API = None
LOGGER = logging.getLogger("STORAGE")

async def get_base_url(config):
    return 'http://{}:{}'.format(config.ipfs.host.value,
                                 config.ipfs.port.value)


async def get_ipfs_gateway_url(config, hash):
    return 'http://{}:{}/ipfs/{}'.format(
        config.ipfs.host.value,
        config.ipfs.gateway_port.value, hash)


async def get_ipfs_api(timeout=60, reset=False):
    global API
    if API is None or reset:
        from aleph.web import app
        host = app['config'].ipfs.host.value
        port = app['config'].ipfs.port.value

        API = aioipfs.AsyncIPFS(host=host, port=port,
                                read_timeout=timeout,
                                conns_max=100)

    return API

async def get_content(message):
    item_type = message.get('item_type', 'ipfs')
    
    if item_type == 'ipfs':
        return await get_json(message['item_hash'])
    elif item_type == 'inline':
        try:
            loop = asyncio.get_event_loop()
            item_content = await loop.run_in_executor(None, json.loads, message['item_content'])
        except (json.JSONDecodeError, KeyError):
            try:
                import json as njson
                item_content = await loop.run_in_executor(None, njson.loads, message['item_content'])
            except (json.JSONDecodeError, KeyError): 
                LOGGER.exception("Can't decode JSON")
                return -1  # never retry, bogus data
        return item_content
    else:
        return None  # unknown, could retry later? shouldn't have arrived this far though.

async def get_json(hash, timeout=1, tries=3):
    from aleph.web import app
    async with aiohttp.ClientSession(read_timeout=timeout) as session:
        uri = await get_ipfs_gateway_url(app['config'], hash)
        try_count = 0
        result = None
        while (result is None) and (try_count < tries):
            try_count += 1
            try:
                async with session.get(uri) as resp:
                    result = await resp.json(content_type=None)
                # result = await api.cat(hash)
                # result = json.loads(result)
            except (concurrent.futures.TimeoutError):
                result = None
                await asyncio.sleep(.5)
            except json.JSONDecodeError:
                result = -1
                break
            except (concurrent.futures.CancelledError,
                    aiohttp.client_exceptions.ClientConnectorError):
                try_count -= 1  # do not count as a try.
                await asyncio.sleep(.1)

        return result


async def add_json(value):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=5)
    # try:
    result = await api.add_json(value)
    # finally:
    #     await api.close()

    return result['Hash']


async def pin_add(hash, timeout=2, tries=3):
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
        except (concurrent.futures.TimeoutError, json.JSONDecodeError):
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
