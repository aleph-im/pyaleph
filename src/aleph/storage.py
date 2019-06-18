""" Storage module for Aleph.
Basically manages the IPFS storage.
"""

import aioipfs
import asyncio
import json
import aiohttp
import concurrent


async def get_base_url(config):
    return 'http://{}:{}'.format(config.ipfs.host.value,
                                 config.ipfs.port.value)


async def get_ipfs_api(timeout=60):
    from aleph.web import app
    host = app['config'].ipfs.host.value
    port = app['config'].ipfs.port.value

    return aioipfs.AsyncIPFS(host=host, port=port, read_timeout=timeout)


async def get_json(hash, timeout=2):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=timeout)
    try:
        result = await api.cat(hash)
        result = json.loads(result)
    except (concurrent.futures.TimeoutError, json.JSONDecodeError):
        result = None
    finally:
        await api.close()

    return result


async def add_json(value):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api()
    try:
        result = await api.add_json(value)
    finally:
        await api.close()

    return result['Hash']


async def pin_add(hash, timeout=5):
    # loop = asyncio.get_event_loop()
    api = await get_ipfs_api(timeout=timeout)
    try:
        result = None
        async for ret in api.pin.add(hash):
            result = ret
    finally:
        await api.close()

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
