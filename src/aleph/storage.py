""" Storage module for Aleph.
Basically manages the IPFS storage.
"""

import asyncio
import json
import logging
from hashlib import sha256

from aleph.services.filestore import get_value, set_value
from aleph.services.ipfs.storage import add_bytes as add_ipfs_bytes
from aleph.services.ipfs.storage import add_file as ipfs_add_file
from aleph.services.ipfs.storage import get_ipfs_content
from aleph.services.ipfs.storage import pin_add as ipfs_pin_add
from aleph.services.p2p.http import request_hash as p2p_http_request_hash
from aleph.services.p2p.protocol import request_hash as p2p_protocol_request_hash
from aleph.web import app

LOGGER = logging.getLogger("STORAGE")

async def get_message_content(message):
    item_type = message.get('item_type', 'ipfs')
    
    if item_type == 'ipfs':
        return await get_json(message['item_hash'], engine='ipfs')
    if item_type == 'storage':
        return await get_json(message['item_hash'], engine='storage')
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
                return -1, 0  # never retry, bogus data
        return item_content,  len(message['item_content'])
    else:
        return None, 0  # unknown, could retry later? shouldn't have arrived this far though.
    
def get_sha256(content):
    if isinstance(content, str):
        content = content.encode('utf-8')
    return sha256(content).hexdigest()

async def get_hash_content(hash, engine='ipfs', timeout=2,
                           tries=1, use_network=True, use_ipfs=True,
                           store_value=True):
    # TODO: determine which storage engine to use
    ipfs_enabled = app['config'].ipfs.enabled.value
    enabled_clients = app['config'].p2p.clients.value
    # content = await loop.run_in_executor(None, get_value, hash)
    content = await get_value(hash)
    if content is None:
        if use_network:
            if 'protocol' in enabled_clients:
                content = await p2p_protocol_request_hash(hash)
                
            if 'http' in enabled_clients and content is None:
                content = await p2p_http_request_hash(hash, timeout=timeout)
        
        if content is not None:
            if engine == 'ipfs' and ipfs_enabled:
                # TODO: get a better way to compare hashes (without depending on IPFS daemon)
                try:
                    compared_hash = await add_ipfs_bytes(content)
                    if compared_hash != hash:
                        LOGGER.warning(f"Got a bad hash! {hash}/{compared_hash}")
                        content = -1
                except asyncio.TimeoutError:
                    LOGGER.warning(f"Can't verify hash {hash}")
                    content = None
                        
            elif engine == 'storage':
                loop = asyncio.get_event_loop()
                compared_hash = await loop.run_in_executor(None, get_sha256, content)
                # compared_hash = sha256(content.encode('utf-8')).hexdigest()
                if compared_hash != hash:
                    LOGGER.warning(f"Got a bad hash! {hash}/{compared_hash}")
                    content = -1
        
        if content is None:
            if ipfs_enabled and engine == 'ipfs' and use_ipfs:
                content = await get_ipfs_content(hash,
                                                 timeout=timeout, tries=tries)
        else:
            LOGGER.info(f"Got content from p2p {hash}")
        
        if content is not None and content != -1 and store_value:
            LOGGER.debug(f"Storing content for{hash}")
            await set_value(hash, content)
    else:
        LOGGER.debug(f"Using stored content for {hash}")
        
    return content

async def get_json(hash, engine='ipfs', timeout=2, tries=1):
    loop = asyncio.get_event_loop()
    content = await get_hash_content(hash, engine=engine,
                                     timeout=timeout, tries=tries)
    size = 0
    if content is not None and content != -1:
        size = len(content)
        try:
            # if len(content) > 100000:
            content = await loop.run_in_executor(None, json.loads, content)
            # else:
            #     content = json.loads(content)
        except json.JSONDecodeError:
            try:
                import json as njson
                content = await loop.run_in_executor(None, njson.loads, content)
            except (json.JSONDecodeError, KeyError):
                LOGGER.exception("Can't decode JSON")
                content = -1  # never retry, bogus data
        
    return content, size

async def pin_hash(chash, timeout=2, tries=1):
    return await ipfs_pin_add(chash, timeout=timeout, tries=tries)

async def add_json(value, engine='ipfs'):
    # TODO: determine which storage engine to use
    loop = asyncio.get_event_loop()
    content = await loop.run_in_executor(None, json.dumps, value)
    content = content.encode('utf-8')
    if engine == 'ipfs':
        chash = await add_ipfs_bytes(content)
    elif engine == 'storage':
        if isinstance(content, str):
            content = content.encode('utf-8')
        chash = sha256(content).hexdigest()
    else:
        raise NotImplementedError('storage engine %s not supported' % engine)
        
    await set_value(chash, content)
    return chash

async def add_file(fileobject, filename=None, engine='ipfs'):
    file_hash = None
    file_content = None
    
    if engine == 'ipfs':
        output = await ipfs_add_file(fileobject, filename)
        file_hash = output['Hash']
        fileobject.seek(0)
        file_content = fileobject.read()
    
    elif engine == 'storage':
        file_content = fileobject.read()
        file_hash = sha256(file_content).hexdigest()
    
    await set_value(file_hash, file_content)
    return file_hash