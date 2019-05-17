""" Storage module for Aleph.
Basically manages the IPFS storage.
"""

import ipfsapi
import asyncio
import aiohttp


async def get_base_url(config):
    return 'http://{}:{}'.format(config.ipfs.host.value,
                                 config.ipfs.port.value)


async def get_ipfs_api():
    from aleph.web import app
    host = app['config'].ipfs.host.value
    port = app['config'].ipfs.port.value

    return ipfsapi.connect(host, port)


async def get_json(hash, timeout=60):
    loop = asyncio.get_event_loop()
    api = await get_ipfs_api()
    future = loop.run_in_executor(
        None, api.get_json, hash)
    result = await asyncio.wait_for(future, timeout, loop=loop)
    return result


async def add_json(value):
    loop = asyncio.get_event_loop()
    api = await get_ipfs_api()
    result = await loop.run_in_executor(
        None, api.add_json, value)
    return result


async def pin_add(hash, timeout=60):
    loop = asyncio.get_event_loop()
    api = await get_ipfs_api()
    future = loop.run_in_executor(
        None, api.pin_add, hash)
    result = await asyncio.wait_for(future, timeout, loop=loop)
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
