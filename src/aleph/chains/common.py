

async def mark_confirmed(chain_name, object_hash, height):
    """ Mark a particular hash as confirmed in underlying chain.
    """
    pass

async def incoming(chain_name, object, hash):
    """ New incoming object from underlying chain.
    Will be marked as confirmed if existing in database, created if not.
    """
    pass

async def invalidate(chain_name, block_height):
    """ Invalidates a particular block height from an underlying chain (in case of forks)
    """
    pass
