""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- hjandle garbage collection of unused hashes
"""

import logging

import aioipfs

from aleph.handlers.register import register_incoming_handler
from aleph.services.ipfs.common import get_ipfs_api
from aleph.storage import get_hash_content
from aleph.web import app

LOGGER = logging.getLogger("HANDLERS.STORAGE")

ALLOWED_ENGINES = ['ipfs', 'storage']

async def handle_new_storage(message, content):
    store_files = app['config'].storage.store_files.value
    if not store_files:
        return True # handled
    
    
    engine = content.get('item_type', None)
    
    if len(content['item_hash']) == 46:
        engine = 'ipfs'
    if len(content['item_hash']) == 64:
        engine = 'storage'
        
    if engine not in ALLOWED_ENGINES:
        LOGGER.warning("Got invalid storage engine %s" % engine)
        return -1 # not allowed, ignore.
    
    file_content = None
    is_folder = False
    item_hash = content['item_hash']
    ipfs_enabled = app['config'].ipfs.enabled.value
    do_standard_lookup = True
    size = 0
    
    if engine == 'ipfs' and ipfs_enabled:
        api = await get_ipfs_api(timeout=1)
        try:
            stats = await api.files.stat(f"/ipfs/{item_hash}")
        
            if stats['Type'] == 'file' and stats['CumulativeSize'] < 5120:
                do_standard_lookup = True
            else:
                size = stats['CumulativeSize']
                content['engine_info'] = stats
                pin_api = await get_ipfs_api(timeout=60)
                timer = 0
                is_folder = stats['Type'] == 'directory'
                async for status in pin_api.pin.add(item_hash):
                    timer += 1
                    if timer > 30 and status['Pins'] is None:
                        return None # Can't retrieve data now.
                do_standard_lookup = False
                
        except aioipfs.APIError as e:
            if "invalid CID" in e.message:
                LOGGER.warning(f"Error retrieving stats of hash {item_hash}: {e.message}")
                return -1
            
            LOGGER.exception(f"Error retrieving stats of hash {item_hash}: {e.message}")
            do_standard_lookup = True
        
    if do_standard_lookup:
        # TODO: We should check the balance here.
        file_content = await get_hash_content(item_hash,
                                        engine=engine, tries=4,
                                        use_network=True, use_ipfs=True,
                                        store_value=True)
        if file_content is None:
            return None
        
        size = len(file_content)
    
    content['size'] = size
    content['content_type'] = is_folder and 'directory' or 'file'
    
    return True
    
    
register_incoming_handler('STORE', handle_new_storage)