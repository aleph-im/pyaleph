import aiohttp
import base64
import base58
import json
import asyncio
import hashlib
from aleph.storage import get_base_url
from aleph.chains.register import VERIFIER_REGISTER
import logging
LOGGER = logging.getLogger("NETWORK")

MAX_INLINE_SIZE = 100000 # 100kb max inline content size.

INCOMING_MESSAGE_AUTHORIZED_FIELDS = [
    'item_hash',
    'item_content',
    'item_type',
    'chain',
    'channel',
    'sender',
    'type',
    'time',
    'signature'
]


async def decode_msg(msg):
    return {
        'from': base58.b58encode(
            base64.b64decode(msg['from'])),
        'data': base64.b64decode(msg['data']),
        'seqno': base58.b58encode(base64.b64decode(msg['seqno'])),
        'topicIDs': msg['topicIDs']
    }


async def incoming_check(ipfs_pubsub_message):
    """ Verifies an incoming message is sane, protecting from spam in the
    meantime.

    TODO: actually implement this, no check done here yet. IMPORTANT.
    """

    try:
        message = json.loads(ipfs_pubsub_message.get('data', ''))
        LOGGER.debug("New message! %r" % message)
        message = await check_message(message, from_network=True)
        return message
    except json.JSONDecodeError:
        LOGGER.exception('Received non-json message %r'
                         % ipfs_pubsub_message.get('data', ''))


async def sub(topic, base_url=None):
    if base_url is None:
        from aleph.web import app
        base_url = await get_base_url(app['config'])

    async with aiohttp.ClientSession(read_timeout=0) as session:
        async with session.get('%s/api/v0/pubsub/sub' % base_url,
                               params={'arg': topic,
                                       'discover': 'true'}) as resp:
            rest_value = None
            while True:
                value, is_full = await resp.content.readchunk()
                if rest_value:
                    value = rest_value + value

                if is_full:
                    # yield value
                    rest_value = None
                    try:
                        mvalue = json.loads(value)
                        mvalue = await decode_msg(mvalue)
                        LOGGER.debug("New message received %r" % mvalue)

                        # we should check the sender here to avoid spam
                        # and such things...
                        message = await incoming_check(mvalue)
                        if message is not None:
                            yield message

                    except Exception:
                        LOGGER.exception("Error handling message")
                else:
                    rest_value = value


async def pub(topic, message, base_url=None):
    if base_url is None:
        from aleph.web import app
        base_url = await get_base_url(app['config'])

    async with aiohttp.ClientSession(read_timeout=0) as session:
        async with session.get('%s/api/v0/pubsub/pub' % base_url,
                               params=(('arg', topic),
                                       ('arg', message))) as resp:
            assert resp.status == 200


async def incoming_channel(config, topic):
    from aleph.chains.common import incoming
    loop = asyncio.get_event_loop()
    while True:
        try:
            i = 0
            seen_ids = []
            tasks = []
            async for message in sub(topic,
                                     base_url=await get_base_url(config)):
                LOGGER.info("New message %r" % message)
                i += 1
                tasks.append(
                    loop.create_task(incoming(message, seen_ids=seen_ids)))

                # await incoming(message, seen_ids=seen_ids)
                if (i > 1000):
                    # every 1000 message we check that all tasks finished
                    # and we reset the seen_ids list.
                    for task in tasks:
                        await task
                    seen_ids = []
                    tasks = []
                    i = 0

        except Exception:
            LOGGER.exception("Exception in IPFS pubsub, reconnecting.")


async def check_message(message, from_chain=False, from_network=False,
                        trusted=False):
    """ This function should check the incoming message and verify any
    extraneous or dangerous information for the rest of the process.
    It also checks the data hash if it's not done by an external provider (ipfs)
    and the data length.
    Example of dangerous data: fake confirmations, fake tx_hash, bad times...
    
    If a item_content is there, set the item_type to inline, else to ipfs (default).

    TODO: Implement it fully! Dangerous!
    """
    if message.get('item_content', None) is not None:
        if len('item_content') > MAX_INLINE_SIZE:
            LOGGER.warning('Message too long')
            return None
        
        if message.get('hash_type', 'sha256') == 'sha256':  # leave the door open.
            h = hashlib.sha256()
            h.update(message['item_content'].encode('utf-8'))
            
            if message['item_hash'] != h.hexdigest():
                LOGGER.warning('Bad hash')
                return None
        else:
            LOGGER.warning('Unknown hash type %s' % message['hash_type'])
            return None
        
        message['item_type'] = 'inline'
        
    else:
        message['item_type'] = 'ipfs'
    
    if trusted:
        # only in the case of a message programmatically built here
        # from legacy native chain signing for example (signing offloaded)
        return message
    else:
        message = {k: v for k, v in message.items()
                   if k in INCOMING_MESSAGE_AUTHORIZED_FIELDS}
        chain = message.get('chain', None)
        signer = VERIFIER_REGISTER.get(chain, None)
        if signer is None:
            LOGGER.warning('Unknown chain for validation %r' % chain)
            return None
        try:
            if await signer(message):
                return message
        except ValueError:
            LOGGER.warning('Signature validation error')
            return None


def setup_listeners(config):
    # for now (1st milestone), we only listen on a single global topic...
    loop = asyncio.get_event_loop()
    loop.create_task(incoming_channel(config, config.aleph.queue_topic.value))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sub('blah', base_url='http://localhost:5001'))
    loop.close()
