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

from aleph.services.ipfs.storage import get_json as get_ipfs_json
from aleph.services.ipfs.storage import add_json as add_ipfs_json
from aleph.services.ipfs.storage import pin_add as ipfs_pin_add

LOGGER = logging.getLogger("STORAGE")

async def get_content(message):
    item_type = message.get('item_type', 'ipfs')
    
    if item_type == 'ipfs':
        return await get_ipfs_json(message['item_hash'])
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
    return await get_ipfs_json(hash, timeout=timeout, tries=tries)

async def pin_hash(hash, timeout=2, tries=1):
    return await ipfs_pin_add(hash, timeout=timeout, tries=tries)

async def add_json(hash):
    # TODO: determine which storage engine to use
    return await add_ipfs_json(hash)