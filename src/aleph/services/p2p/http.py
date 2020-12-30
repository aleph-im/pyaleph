""" While our own streamer libp2p protocol is still unstable, use direct
HTTP connection to standard rest API.
"""

import base64
import logging
from random import sample

import aiohttp
import asyncio

from . import singleton

LOGGER = logging.getLogger('P2P.HTTP')

SESSIONS = dict()

async def api_get_request(base_uri, method, timeout=1):
    if timeout not in SESSIONS:
        connector = aiohttp.TCPConnector(limit_per_host=5)
        SESSIONS[timeout] = aiohttp.ClientSession(read_timeout=timeout,
                                                  connector=connector)
        
    uri = f"{base_uri}/api/v0/{method}"
    try:
        async with SESSIONS[timeout].get(uri) as resp:
            if resp.status != 200:
                result = None
            else:
                result = await resp.json()
    except (TimeoutError, asyncio.TimeoutError, ConnectionRefusedError):
        result = None
    except Exception:
        LOGGER.exception("Error in retrieval")
        result = None
    return result


async def get_peer_hash_content(base_uri, item_hash, timeout=1):
    result = None
    item = await api_get_request(base_uri, f"storage/{item_hash}",
                                 timeout=timeout)
    if item is not None and item['status'] == 'success' and item['content'] is not None:
        # TODO: IMPORTANT /!\ verify the hash of received data!
        return base64.decodebytes(item['content'].encode('utf-8'))
    else:
        LOGGER.debug(f"can't get hash {item_hash}")

    return result
    
    
async def request_hash(item_hash, timeout=1):
    if singleton.api_servers is None:
        return None
    
    uris = sample(singleton.api_servers, k=len(singleton.api_servers))
    for uri in uris:
        content = await get_peer_hash_content(uri, item_hash, timeout=timeout)
        if content is not None:
            return content
        
    return None # Nothing found...
