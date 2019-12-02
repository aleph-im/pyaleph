""" This is the storage message handlers file.

For now it's very simple, we check if we want to store files or not.

TODO:
- check balances and storage allowance
- handle incentives from 3rd party
- hjandle garbage collection of unused hashes
"""

from aleph.web import app
from aleph.handlers.register import register_incoming_handler
from aleph.storage import get_hash_content

ALLOWED_ENGINES = ['ipfs', 'storage']

async def handle_new_storage(message, content):
    store_files = app['config'].storage.store_files.value
    if not store_files:
        return True # handled
    
    
    engine = content.get('item_type', None)
    if engine not in ALLOWED_ENGINES:
        LOGGER.warning("Got invalid storage engine %s" % engine)
        return -1 # not allowed, ignore.
    
    # TODO: We should check the balance here.
    file_content = await get_hash_content(content['item_hash'],
                                     engine=engine, tries=4,
                                     use_network=True, use_ipfs=True,
                                     store_value=True)
    if file_content is None:
        return None # can't handle it for now.
    
    size = len(file_content)
    content['size'] = size
    return True
    
    
register_incoming_handler('STORE', handle_new_storage)