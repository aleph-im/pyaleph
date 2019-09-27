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

from aleph.services.ipfs.storage import get_ipfs_content
from aleph.services.ipfs.storage import add_json as add_ipfs_json
from aleph.services.ipfs.storage import add_bytes as add_ipfs_bytes
from aleph.services.ipfs.storage import pin_add as ipfs_pin_add
from aleph.services.p2p.protocol import request_hash
from aleph.services.filestore import get_value, set_value
from aleph.web import app

LOGGER = logging.getLogger("STORAGE")

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
    
async def get_json(hash, timeout=1, tries=1):
    # TODO: determine which storage engine to use
    ipfs_enabled = app['config'].ipfs.enabled.value
    loop = asyncio.get_event_loop()
    # content = await loop.run_in_executor(None, get_value, hash)
    content = await get_value(hash)
    if content is None:
        content = await request_hash(hash)
        
        if content is not None and ipfs_enabled:
            # TODO: get a better way to compare hashes (without depending on IPFS daemon)
            compared_hash = await add_ipfs_bytes(content)
            if compared_hash != hash:
                LOGGER.warning("Got a bad hash! {hash}/{compared_hash}")
                content = None
        
        if content is None:
            if ipfs_enabled:
                content = await get_ipfs_content(hash, timeout=timeout, tries=tries)
        else:
            LOGGER.debug(f"Got content fron p2p {hash}")
        
        if content is not None and content != -1:
            LOGGER.debug(f"Storing content for{hash}")
            await set_value(hash, content)
    else:
        LOGGER.debug(f"Using stored content for {hash}")
            
    if content is not None and content != -1:
        try:
            if len(content) > 100000:
                content = await loop.run_in_executor(None, json.loads, content)
            else:
                content = json.loads(content)
        except json.JSONDecodeError:
            try:
                import json as njson
                content = await loop.run_in_executor(None, njson.loads, content)
            except (json.JSONDecodeError, KeyError): 
                LOGGER.exception("Can't decode JSON")
                content = -1  # never retry, bogus data
        
    return content

async def pin_hash(chash, timeout=2, tries=1):
    return await ipfs_pin_add(chash, timeout=timeout, tries=tries)

async def add_json(value):
    # TODO: determine which storage engine to use
    loop = asyncio.get_event_loop()
    content = await loop.run_in_executor(None, json.dumps, value)
    chash = await add_ipfs_bytes(content)
    # await loop.run_in_executor(None, json.dumps, value)
    await set_value(chash, content)
    return chash
    