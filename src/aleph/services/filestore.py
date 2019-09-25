import rocksdb
import os

HASHES_STORAGE = 'hashes'

hashes_db = None

def init_store(config):
    global hashes_db
    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 300000
    opts.write_buffer_size = 67108864
    opts.max_write_buffer_number = 3
    opts.target_file_size_base = 67108864

    opts.table_factory = rocksdb.BlockBasedTableFactory(
        filter_policy=rocksdb.BloomFilterPolicy(10),
        block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
        block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

    hashes_db = rocksdb.DB(os.path.join(config.storage.folder.value, HASHES_STORAGE), opts)
    
async def get_value(key):
    # if not isinstance(key, bytes):
    #     if isinstance(key, str):
    #         key = key.encode('utf-8')
    #     else:
    #         raise ValueError('Bad input key (bytes or string only)')    
        
    return hashes_db.get(key.encode('utf-8'))

async def set_value(key, value):
    if not isinstance(key, bytes):
        if isinstance(key, str):
            key = key.encode('utf-8')
        else:
            raise ValueError('Bad input key (bytes or string only)')    
        
    if not isinstance(value, bytes):
        if isinstance(value, str):
            value = value.encode('utf-8')
        else:
            raise ValueError('Bad input value (bytes or string only)')    
    
    return hashes_db.put(key, value)