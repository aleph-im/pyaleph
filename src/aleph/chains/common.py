from aleph.storage import get_json, pin
from aleph.model.messages import Message

LOGGER = logging.getLogger('chains.common')

async def mark_confirmed(chain_name, object_hash, height):
    """ Mark a particular hash as confirmed in underlying chain.
    """
    pass

async def incoming(chain_name, message):
    """ New incoming message from underlying chain.
    Will be marked as confirmed if existing in database, created if not.
    """
    hash = message['hash']
    message['chain'] = message.get('chain', chain_name) # we set the incoming chain as default for signature
    try:
        content = await get_json(hash)

        # for now, only support direct signature (no 3rd party or multiple address signing)
        if message['sender'] != content['address']:
            LOGGER.warn("Invalid sender for %s" % hash)
            return

        # since it's on-chain, we need to keep that content.
        # TODO: verify signature before (in chain-specific stage?)
        await pin(hash)

    except Exception as exc:
        LOGGER.exception("Can't get content of object %r" % hash)
        return

    dbmsg = Message.collection.find_one()



async def invalidate(chain_name, block_height):
    """ Invalidates a particular block height from an underlying chain (in case of forks)
    """
    pass
